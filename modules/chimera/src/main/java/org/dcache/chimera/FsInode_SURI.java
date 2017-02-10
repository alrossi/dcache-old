/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.URI;
import java.util.List;

import org.dcache.chimera.posix.Stat;

public class FsInode_SURI extends FsInode {
    private static final String NEWLINE = "\n\r";
    Logger logger = LoggerFactory.getLogger(FsInode_SURI.class);
    private List<StorageLocatable> locations;

    /**
     * @param fs  pointer to 'File System'
     * @param ino inode number of the
     */
    public FsInode_SURI(FileSystemProvider fs, long ino, FsInodeType type) {
        super(fs, ino, type);
    }

    @Override
    public boolean exists() {
        try {
            locations = _fs.getInodeLocations(this,
                                              StorageGenericLocation.TAPE);
        } catch (Exception e) {
            /*
             * Swallow exception.
             *
             * This follows the model of FsInode_TAG.exists().
             */
        }
        return locations != null;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public boolean isLink() {
        return false;
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {
        logger.error("READ, pos {}, offset {}, len {}", pos, offset, len);
        String locations;

        try {
            locations = getLocations() + NEWLINE;
        } catch (ChimeraFsException e) {
            locations = "";
        }

        byte[] b = locations.getBytes();

        if (pos > b.length) {
            return 0;
        }

        int copyLen = Math.min(len, b.length - (int) pos);
        System.arraycopy(b, (int) pos, data, 0, copyLen);

        return copyLen;
    }

    @Override
    public Stat stat() throws ChimeraFsException {
        Stat ret = super.stat();
        ret.setSize(getLocations().length()+1);
        ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);
        // invalidate NFS cache
        ret.setMTime(System.currentTimeMillis());
        return ret;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len)
                    throws ChimeraFsException {
        String uid = MDC.get("nfs.principal");

        logger.error("WRITE: pos {}, offset {}, len {}; uid {}", pos, offset, len, uid);

        if (!getLocations().isEmpty() && !"0".equals(uid)) {
            throw new PermissionDeniedChimeraFsException("User not allowed to overwrite.");
        }

        if (pos == 0) {
            _fs.clearInodeLocation(this,
                                   StorageGenericLocation.TAPE,
                                   null); // all tape locations
        }

        /*
         * If there is a trailing NL, get rid of it.
         */
        String uriString = new String(data, offset, len).trim();

        if (uriString.length() == 0) {
            return 0;
        }

        /*
         * Validate uri syntax.
         */
        uriString = URI.create(uriString).toString();

        _fs.addInodeLocation(this,
                             StorageGenericLocation.TAPE,
                             uriString);

        return Math.max(len, uriString.length());
    }

    private String getLocations() throws ChimeraFsException {
        if (!exists()) {
            throw new IOHimeraFsException(
                            "Cannot find location information for "
                                            + getId());
        }

        String result = Joiner.on(NEWLINE)
                              .join(locations.stream()
                                             .map(StorageLocatable::location)
                                             .filter((l) -> l.trim().length()
                                                             != 0)
                                             .toArray());
        logger.error("{} calling getLocations(), result {}", ino(), result);
        return result;
    }
}
