/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021-2022 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.util;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.dcache.acl.ACL;
import org.dcache.namespace.FileType;
import org.dcache.vehicles.FileAttributes;

/**
 * A fluent class to build a FileAttributes object.
 */
public class FileAttributesBuilder {

    private final FileAttributes _attributes = new FileAttributes();
    private final Set<Checksum> _checksums = new HashSet<>();

    public static FileAttributesBuilder fileAttributes() {
        return new FileAttributesBuilder();
    }

    public FileAttributesBuilder withAcl(ACL acl) {
        _attributes.setAcl(acl);
        return this;
    }

    public FileAttributesBuilder withSize(long size) {
        _attributes.setSize(size);
        return this;
    }

    public FileAttributesBuilder withSize(long size, ByteUnit units) {
        return withSize(units.toBytes(size));
    }

    public FileAttributesBuilder withCtime(long ctime) {
        _attributes.setChangeTime(ctime);
        return this;
    }

    public FileAttributesBuilder withCreationTime(long creationTime) {
        _attributes.setCreationTime(creationTime);
        return this;
    }

    public FileAttributesBuilder withAtime(long atime) {
        _attributes.setAccessTime(atime);
        return this;
    }

    public FileAttributesBuilder withMtime(long mtime) {
        _attributes.setModificationTime(mtime);
        return this;
    }

    public FileAttributesBuilder withOwner(int owner) {
        _attributes.setOwner(owner);
        return this;
    }

    public FileAttributesBuilder withGroup(int group) {
        _attributes.setGroup(group);
        return this;
    }

    public FileAttributesBuilder withMode(int mode) {
        _attributes.setMode(mode);
        return this;
    }

    public FileAttributesBuilder withNlink(int nlink) {
        _attributes.setNlink(nlink);
        return this;
    }

    public FileAttributesBuilder withAccessLatency(AccessLatency accessLatency) {
        _attributes.setAccessLatency(accessLatency);
        return this;
    }

    public FileAttributesBuilder withRetentionPolicy(RetentionPolicy retentionPolicy) {
        _attributes.setRetentionPolicy(retentionPolicy);
        return this;
    }

    public FileAttributesBuilder withStorageClass(String storageClass) {
        _attributes.setStorageClass(storageClass);
        return this;
    }

    public FileAttributesBuilder withHsm(String hsm) {
        _attributes.setHsm(hsm);
        return this;
    }

    public FileAttributesBuilder withCacheClass(String cacheClass) {
        _attributes.setCacheClass(cacheClass);
        return this;
    }

    public FileAttributesBuilder withLabel(String name) {
        return withLabels(Collections.singleton(name));
    }

    public FileAttributesBuilder withLabels(Set<String> names) {
        _attributes.setLabels(names);
        return this;
    }

    public FileAttributesBuilder withLabels(String ... labels) {
        withLabels(Arrays.stream(labels).collect(Collectors.toSet()));
        return this;
    }

    public FileAttributesBuilder withType(FileType type) {
        _attributes.setFileType(type);
        return this;
    }

    public FileAttributesBuilder withId(PnfsId id) {
        _attributes.setPnfsId(id);
        return this;
    }

    public FileAttributesBuilder withLocations(String ... locations) {
        _attributes.setLocations(Arrays.stream(locations).collect(Collectors.toSet()));
        return this;
    }

    public FileAttributesBuilder withStorageInfo(StorageInfoBuilder builder) {
        return withStorageInfo(builder.build());
    }

    public FileAttributesBuilder withStorageInfo(StorageInfo info) {
        _attributes.setStorageInfo(info);
        return this;
    }

    public FileAttributesBuilder withXattr(String name, String value) {
        _attributes.updateXattr(name, value);
        return this;
    }

    public FileAttributesBuilder withXattr(String ... xattrs) {
        for (String xattr: xattrs) {
            String[] nvpair = xattr.split("[:]");
            withXattr(nvpair[0], nvpair[1]);
        }
        return this;
    }

    public FileAttributesBuilder withFlags(String ... flags) {
        Map<String, String> map = new HashMap<>();
        for (String flag: flags) {
            String[] nvpair = flag.split("[:]");
            map.put(nvpair[0], nvpair[1]);
        }
        _attributes.setFlags(map);
        return this;
    }

    public FileAttributesBuilder withChecksum(Checksum checksum) {
        _checksums.add(checksum);
        return this;
    }

    public FileAttributesBuilder withChecksums(Checksum ... checksums) {
        Arrays.stream(checksums).forEach(c -> _checksums.add(c));
        return this;
    }

    public FileAttributes build() {
        if (!_checksums.isEmpty()) {
            _attributes.setChecksums(_checksums);
        }
        return _attributes;
    }
}
