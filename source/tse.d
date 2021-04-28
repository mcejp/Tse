/*
Transactional Storage Engine
Martin Cejp, 2021

Transactions work like this:
  A transaction logs all changes into a write-ahead log (WAL).
  Upon commiting the transaction, it is marked as commited in the WAL.
  Only when the Checkpoint operation is called, the changes are written back to the main file.
  Since in this incarnation there is no page cache, changes have to be checkpointed before any read operation.

TODO: https://dlang.org/dstyle.html
*/

module tse;

import std.digest.crc;
import std.experimental.logger;
import std.file;
import std.path;
import std.string;
import std.typecons;

import core.sys.posix.fcntl;
import core.sys.posix.stdio;
import core.sys.posix.sys.stat;
import core.sys.posix.unistd;

private enum default_permissions = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH;	// same as fopen() on Linux

public class Wal {
    private int fd;

    private int currentTxId = -1;
    private int nextTxId;

    private off_t txStartOffset;
    private uint txLen;

    private CRC32 crc;

    private struct TxHeader {
        uint32_t txId;
        uint32_t txLen;
        ubyte[4] txCrc;
    };

    private struct EntryHeader {
        uint16_t fileId;
        uint32_t fileOffset;
        uint32_t length;
    }

    this(int fd) {
        this.fd = fd;

        this.nextTxId = -1;
    }

    public int beginTransaction()
        in(this.currentTxId < 0)
        in(this.nextTxId >= 0)
    {
        currentTxId = nextTxId;
        nextTxId++;

        // TODO: write the header
        TxHeader hdr = {
            txId: currentTxId,
            txLen: 0,
            txCrc: 0,
        };

        tracef("tx=%d", hdr.txId);
        this.txStartOffset = lseek(this.fd, 0, SEEK_CUR); // FIXME: check return
        write(this.fd, &hdr, hdr.sizeof);       // FIXME: check return

        this.txLen = hdr.sizeof;
        crc.start();

        return currentTxId;
    }

    public void commit(int transaction)
    in(transaction == currentTxId)
    {
        // Update txLen & txCrc

        // negative relative lseek broken?
        lseek(this.fd, lseek(this.fd, 0, 1) - txLen, SEEK_SET);

        TxHeader hdr = {
            txId: currentTxId,
            txLen: this.txLen,
            txCrc: crc.finish(),
        };

        tracef("tx=%d len=%d crc=%s", hdr.txId, hdr.txLen, crcHexString(hdr.txCrc));
        write(this.fd, &hdr, hdr.sizeof);       // FIXME: check return

        // Sync file
        fdatasync(this.fd);

        // At this moment, the transaction is commited, and therefore binding.
        // If we crash anytime from now on, the changes will be applied from the hot journal.

        // Return to end of file
        lseek(this.fd, 0, SEEK_END);// FIXME: check return

        currentTxId = -1;
    }

    public void dump()
    in(currentTxId < 0)
    {
        lseek(this.fd, 0, SEEK_SET);

        for (;;) {
            // Read tx header
            TxHeader txHdr;
            auto nread = read(this.fd, &txHdr, txHdr.sizeof);

            if (nread == 0) {
                break;
            }

            assert(nread == txHdr.sizeof);

            infof("- Tx [txId=%d len=%d crc=%s]", txHdr.txId, txHdr.txLen, crcHexString(txHdr.txCrc));

            int payloadRead = 0;
            CRC32 crc;

            while (payloadRead < txHdr.txLen) {
                EntryHeader entryHdr;
                assert(payloadRead + entryHdr.sizeof < txHdr.txLen);

                // Read entry header
                nread = read(this.fd, &entryHdr, entryHdr.sizeof);
                assert(nread == entryHdr.sizeof);
                payloadRead += entryHdr.sizeof;
                crc.put((cast(const ubyte*) &entryHdr)[0..entryHdr.sizeof]);

                infof("--- Entry [file=%d off=%d len=%d]", entryHdr.fileId, entryHdr.fileOffset, entryHdr.length);

                assert(payloadRead + entryHdr.length <= txHdr.txLen);

                auto buf = new ubyte[entryHdr.length];
                nread = read(this.fd, buf.ptr, buf.length);
                assert(nread == buf.length);
                crc.put(buf);

                payloadRead += buf.length;
            }

            if (crc.finish() == txHdr.txCrc) {
                info("- Tx CRC VALID");
            }
            else {
                info("- Tx CRC invalid");
            }
        }
    }

    public void rollback(int transaction)
    in(transaction == currentTxId) {
        // If this is the last transaction in the file, we can just seek back & truncate

        currentTxId = -1;
    }

    private void writeTxPayload(const ubyte[] bytes) {
        // FIXME: check returned value
        write(fd, bytes.ptr, bytes.length);
        this.crc.put(bytes);
        this.txLen += bytes.length;
    }

    public int writeChange(int transaction, int fileId, int fileOffset, const ubyte[] newData)
    in(currentTxId == transaction)
    in(fileId < uint16_t.max)
    in(newData.length < uint32_t.max)
    {
        tracef("tx=%d file=%d off=%d new=%s", transaction, fileId, fileOffset, newData);

        EntryHeader eh = {
            fileId: cast(uint16_t) fileId,
            fileOffset: fileOffset,
            length: cast(uint32_t) newData.length,
        };

        // FIXME: check returned value
        writeTxPayload((cast(const ubyte*) &eh) [0..eh.sizeof]);
        writeTxPayload(newData);

        return 0;
    }

    public void checkpoint(void delegate(int fileId, int fileOffset, const ubyte[] bytes) changeCallback, void delegate() flushCallback)
    in(currentTxId < 0)
    {
        lseek(this.fd, 0, SEEK_SET);

        for (;;) {
            // Read tx header
            TxHeader txHdr;
            auto nread = read(this.fd, &txHdr, txHdr.sizeof);

            if (nread == 0) {
                break;
            }

            assert(nread == txHdr.sizeof);

            infof("- Tx [txId=%d len=%d crc=%s]", txHdr.txId, txHdr.txLen, crcHexString(txHdr.txCrc));

            if (txHdr.txLen == 0) {
                info("- Tx abandoned");

                // Assume Tx rolled back, stop processing
                break;
            }

            int payloadRead = 0;
            CRC32 crc;

            off_t start = lseek(this.fd, 0, SEEK_CUR);

            for (payloadRead = txHdr.sizeof; payloadRead < txHdr.txLen; ) {
                EntryHeader entryHdr;
                assert(payloadRead + entryHdr.sizeof < txHdr.txLen);

                // Read entry header
                nread = read(this.fd, &entryHdr, entryHdr.sizeof);
                assert(nread == entryHdr.sizeof);
                payloadRead += entryHdr.sizeof;
                crc.put((cast(const ubyte*) &entryHdr)[0..entryHdr.sizeof]);

                infof("--- Entry [file=%d off=%d len=%d]", entryHdr.fileId, entryHdr.fileOffset, entryHdr.length);

                assert(payloadRead + entryHdr.length <= txHdr.txLen);

                auto buf = new ubyte[entryHdr.length];
                nread = read(this.fd, buf.ptr, buf.length);
                assert(nread == buf.length);
                crc.put(buf);

                payloadRead += buf.length;
            }

            if (crc.finish() == txHdr.txCrc) {
                info("- Tx CRC VALID");
            }
            else {
                info("- Tx CRC invalid");

                // We stop processing here, assuming following is an unfinished transaction
                break;
            }

            lseek(this.fd, start, SEEK_SET);

            // TODO: theoretical check-to-use race condition
            for (payloadRead = txHdr.sizeof; payloadRead < txHdr.txLen; ) {
                EntryHeader entryHdr;
                assert(payloadRead + entryHdr.sizeof < txHdr.txLen);

                // Read entry header
                nread = read(this.fd, &entryHdr, entryHdr.sizeof);
                assert(nread == entryHdr.sizeof);
                payloadRead += entryHdr.sizeof;

                assert(payloadRead + entryHdr.length <= txHdr.txLen);

                auto buf = new ubyte[entryHdr.length];
                nread = read(this.fd, buf.ptr, buf.length);
                assert(nread == buf.length);

                changeCallback(entryHdr.fileId, entryHdr.fileOffset, buf);

                payloadRead += buf.length;
            }
        }

        flushCallback();

        lseek(this.fd, 0, SEEK_SET);
        ftruncate(this.fd, 0);
        this.nextTxId = 0;
    }
}


public class FileBackingStore {
    private string baseDirectory;

    private Wal wal;

    private int[int] fds;

    this(string baseDirectory, Wal wal) {
        this.baseDirectory = baseDirectory;

        this.wal = wal;
    }

    void close() {
        foreach (fileId, fd; fds) {
            core.sys.posix.unistd.close(fd);
        }

        fds.clear();
    }

    void flush() {
        foreach (fileId, fd; fds) {
            fsync(fd);
        }
    }

    void readRaw(int fileId, int fileOffset, ubyte[] buffer) {
        int fd = getFd(fileId);
        tracef("readRaw fileId=%d fileOffset=%d length=%d", fileId, fileOffset, buffer.length);
        pread(fd, buffer.ptr, buffer.length, fileOffset);   // FIXME: check return
    }

    void writeRaw(int fileId, int fileOffset, const ubyte[] buffer) {
        int fd = getFd(fileId);
        tracef("writeRaw fileId=%d fileOffset=%d length=%d", fileId, fileOffset, buffer.length);
        pwrite(fd, buffer.ptr, buffer.length, fileOffset);   // FIXME: check return
    }

    int getFd(int fileId) {
        int* p_fd = fileId in fds;

        if (p_fd) {
            return *p_fd;
        }
        else {
            auto path = buildPath(baseDirectory, format("%d.bin", fileId));
            int fd = open(toStringz(path), O_CREAT | O_RDWR, default_permissions);
            // FIXME: error check
            fds[fileId] = fd;
            return fd;
        }
    }
}


public class TSE {
    private int walFd;
    private Wal wal;
    private FileBackingStore pm;

    this(string path) {
        mkdirRecurse(path);
        this.walFd = open(toStringz(buildPath(path, "wal.bin")), O_CREAT | O_RDWR, default_permissions);
        this.wal = new Wal(walFd);

        this.pm = new FileBackingStore(path, wal);

        this.checkpoint();
    }

    void close() {
        // core.sys.posix.unistd.close(this.fd);
        core.sys.posix.unistd.close(this.walFd);
    }

    int beginTransaction() {
        return this.wal.beginTransaction();
    }

    void checkpoint() {
        wal.checkpoint((int fileId, int fileOffset, const ubyte[] bytes) {
            this.pm.writeRaw(fileId, fileOffset, bytes);
        }, () {
            // Before the WAL is truncated, file changes must be synced to disk
            this.pm.flush();
        });
    }

    void commit(int transaction) {
        this.wal.commit(transaction);
        this.checkpoint();
    }

    void rollback(int transaction) {
        this.wal.rollback(transaction);
        this.checkpoint();
    }

    void getBytes(int fileId, int fileOffset, ubyte[] buffer) {
        return this.pm.readRaw(fileId, fileOffset, buffer);
    }

    // Within a transaction, add a range of changed bytes
    // File size is NOT TRACKED -> rolling back and append will overwrite with 0xCC bytes
    void putBytes(int transaction, int fileId, int fileOffset, const ubyte[] newData) {
        tracef("putRange(tx=%d fileId=%d fileOffset=%d len=%d data=%s)", transaction, fileId, fileOffset, newData.length, newData);

        wal.writeChange(transaction, fileId, fileOffset, newData);
    }
}

unittest
{
    auto tse = new TSE("my_db.tse");
    auto message = cast(ubyte[]) "Hello, world";
    auto message2 = cast(ubyte[]) "Byebye";

    // Write & commit hello
    int tx = tse.beginTransaction();
    tse.putBytes(tx, 1, 20, message);
    tse.commit(tx);

    // Check hello
    {
        ubyte[12] buffer;
        tse.getBytes(1, 20, buffer);
        assert(buffer == message);
    }

    // Write new
    tx = tse.beginTransaction();
    tse.putBytes(tx, 1, 20, message2);

    // Check not overwritten
    // FIXME: this is actually wrong -- we *should* see the changes immediately
    {
        ubyte[12] buffer;
        tse.getBytes(1, 20, buffer);
        assert(buffer == message);
    }

    // Now commit
    tse.commit(tx);

    // Check that it has been written finally
    {
        ubyte[12] buffer;
        tse.getBytes(1, 20, buffer);
        assert(buffer[0..message2.length] == message2);
        assert(buffer[message2.length..$] == message[message2.length..$]);
    }

    // Start another tx, but roll back
    tx = tse.beginTransaction();
    tse.putBytes(tx, 1, 20, message);
    tse.rollback(tx);

    // Check nothing was written
    {
        ubyte[12] buffer;
        tse.getBytes(1, 20, buffer);
        assert(buffer[0..message2.length] == message2);
        assert(buffer[message2.length..$] == message[message2.length..$]);
    }

    tse.close();
}
