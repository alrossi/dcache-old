/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.namespace.replication.db;

import com.google.common.collect.Range;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.security.auth.Subject;
import javax.sql.DataSource;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import org.dcache.chimera.BackEndErrorHimeraFsException;
import org.dcache.chimera.IOHimeraFsException;
import org.dcache.commons.util.SqlHelper;
import org.dcache.namespace.CreateOption;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.ListHandler;
import org.dcache.util.ChecksumType;
import org.dcache.util.Glob;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.commons.util.SqlHelper.tryToClose;

/**
 * Wraps both the standard namespace provider as well as a few specialized
 * queries requiring direct access to the underlying database.
 * <p/>
 * The extra queries return the pnfsids for a given location, and
 * optionally counts for them representing total number of replicas.
 * <p/>
 *
 * Created by arossi on 1/31/15.
 */
public class LocalNamespaceAccess implements NameSpaceProvider {
    private static final String SQL_GET_PNFSIDS_FOR_LOCATION =
                    "SELECT t1.ipnfsid "
                                    + "FROM t_locationinfo t1 "
                                    + "WHERE t1.itype = 1 "
                                    + "AND t1.ilocation = ? "
                                    + "ORDER BY t1.ipnfsid";

    private static final String SQL_GET_LOCATION_COUNTS =
                    "SELECT t1.ipnfsid, count(*) "
                                    + "FROM t_locationinfo t1, t_locationinfo t2 "
                                    + "WHERE t1.ipnfsid = t2.ipnfsid "
                                    + "AND t1.itype = 1 AND t2.itype = 1 "
                                    + "AND t2.ilocation = ? "
                                    + "GROUP BY t1.ipnfsid";

    /**
     * Database connection pool for queries returning multiple pnfsid
     * location info.
     */
    private DataSource connectionPool;

    /**
     * Delegate service used to extract file attributes and
     * single pnfsId location info.
     */
    private NameSpaceProvider namespace;

    public void setConnectionPool(DataSource connectionPool) {
        this.connectionPool = connectionPool;
    }

    public void setNamespace(NameSpaceProvider namespace) {
        this.namespace = namespace;
    }

    public Collection<String> getAllPnfsidsFor(String location)
                    throws CacheException {
        String sql = SQL_GET_PNFSIDS_FOR_LOCATION;
        try {
            Connection dbConnection = getConnection();
            try {
                Map<String, Integer> results = new TreeMap<>();
                getResult(dbConnection, sql, location, results);
                return results.keySet();
            } catch (SQLException e) {
                throw new IOHimeraFsException(e.getMessage());
            } finally {
                tryToClose(dbConnection);
            }
        } catch (IOHimeraFsException e) {
            throw new CacheException(CacheException.RESOURCE,
                            String.format("Could not load cache"
                                           + " locations for %s.",
                                            location),
                            e);
        }
    }

    /**
     * @param location pool name.
     * @return map of pnfsid, replica count entries.
     * @throws CacheException
     */
    public Map<String, Integer> getPnfsidCountsFor(String location)
                    throws CacheException {
        String sql = SQL_GET_LOCATION_COUNTS + " ORDER BY count(*)";
        return getResult(location, null);
    }

    /**
     * @param location pool name.
     * @param filter integer inequality such as >= 3, < 2, = 1.
     * @return map of pnfsid, replica count entries.
     * @throws CacheException
     */
    public Map<String, Integer> getPnfsidCountsFor(String location,
                                                        String filter)
                    throws CacheException, ParseException {
        validateInequality(filter);
        String sql = sql = String.format(SQL_GET_LOCATION_COUNTS
                        + " HAVING count(*) %s", filter);;
        return getResult(location, filter);
    }

    /*
     * ********************* NamespaceProvider API **************************
     */

    @Override
    public FileAttributes createFile(Subject subject, String path, int uid,
                    int gid, int mode, Set<FileAttribute> requestedAttributes)
                    throws CacheException {
        return namespace.createFile(subject, path, uid, gid, mode,
                        requestedAttributes);
    }

    @Override
    public PnfsId createDirectory(Subject subject, String path, int uid,
                    int gid, int mode) throws CacheException {
        return namespace.createDirectory(subject, path, uid, gid, mode);
    }

    @Override
    public PnfsId createSymLink(Subject subject, String path, String dest,
                    int uid, int gid) throws CacheException {
        return namespace.createSymLink(subject, path, dest, uid, gid);
    }

    @Override public void deleteEntry(Subject subject, PnfsId pnfsId)
                    throws CacheException {
        namespace.deleteEntry(subject, pnfsId);
    }

    @Override public void deleteEntry(Subject subject, String path)
                    throws CacheException {
        namespace.deleteEntry(subject, path);
    }

    @Override
    public void renameEntry(Subject subject, PnfsId pnfsId, String newName,
                    boolean overwrite) throws CacheException {
        namespace.renameEntry(subject, pnfsId, newName, overwrite);
    }

    @Override public String pnfsidToPath(Subject subject, PnfsId pnfsId)
                    throws CacheException {
        return namespace.pnfsidToPath(subject, pnfsId);
    }

    @Override public PnfsId pathToPnfsid(Subject subject, String path,
                    boolean followLinks) throws CacheException {
        return namespace.pathToPnfsid(subject, path, followLinks);
    }

    @Override public PnfsId getParentOf(Subject subject, PnfsId pnfsId)
                    throws CacheException {
        return namespace.getParentOf(subject, pnfsId);
    }

    @Override public void removeFileAttribute(Subject subject, PnfsId pnfsId,
                    String attribute) throws CacheException {
        namespace.removeFileAttribute(subject, pnfsId, attribute);
    }

    @Override public void removeChecksum(Subject subject, PnfsId pnfsId,
                    ChecksumType type) throws CacheException {
        namespace.removeChecksum(subject, pnfsId, type);
    }

    @Override public void addCacheLocation(Subject subject, PnfsId pnfsId,
                    String cacheLocation) throws CacheException {
        namespace.addCacheLocation(subject, pnfsId, cacheLocation);
    }

    @Override
    public List<String> getCacheLocation(Subject subject, PnfsId pnfsId)
                    throws CacheException {
        return namespace.getCacheLocation(subject, pnfsId);
    }

    @Override public void clearCacheLocation(Subject subject, PnfsId pnfsId,
                    String cacheLocation, boolean removeIfLast)
                    throws CacheException {
        namespace.clearCacheLocation(subject, pnfsId, cacheLocation,
                        removeIfLast);
    }

    @Override
    public FileAttributes getFileAttributes(Subject subject, PnfsId pnfsId,
                    Set<FileAttribute> attr) throws CacheException {
        return namespace.getFileAttributes(subject, pnfsId, attr);
    }

    @Override
    public FileAttributes setFileAttributes(Subject subject, PnfsId pnfsId,
                    FileAttributes attr, Set<FileAttribute> fetch)
                    throws CacheException {
        return namespace.setFileAttributes(subject, pnfsId, attr, fetch);
    }

    @Override public void list(Subject subject, String path, Glob glob,
                    Range<Integer> range, Set<FileAttribute> attrs,
                    ListHandler handler) throws CacheException {
        namespace.list(subject, path, glob, range, attrs, handler);
    }

    @Override public FsPath createUploadPath(Subject subject, FsPath path,
                    FsPath rootPath, int uid, int gid, int mode, Long size,
                    AccessLatency al, RetentionPolicy rp, String spaceToken,
                    Set<CreateOption> options) throws CacheException {
        return namespace.createUploadPath(subject, path, rootPath, uid, gid,
                        mode, size, al, rp, spaceToken, options);
    }

    @Override
    public PnfsId commitUpload(Subject subject, FsPath uploadPath, FsPath path,
                    Set<CreateOption> options) throws CacheException {
        return namespace.commitUpload(subject, uploadPath, path, options);
    }

    @Override
    public void cancelUpload(Subject subject, FsPath uploadPath, FsPath path)
                    throws CacheException {
        namespace.cancelUpload(subject, uploadPath, path);
    }


    private Connection getConnection() throws IOHimeraFsException {
        try {
            Connection dbConnection = connectionPool.getConnection();
            dbConnection.setAutoCommit(true);
            return dbConnection;
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }
    }

    private Map<String, Integer> getResult(String query, String location)
                    throws CacheException {
        try {
            Connection dbConnection = getConnection();
            try {
                Map<String, Integer> results = new TreeMap<>();
                getResult(dbConnection, query, location, results);
                return results;
            } catch (SQLException e) {
                throw new IOHimeraFsException(e.getMessage());
            } finally {
                tryToClose(dbConnection);
            }
        } catch (IOHimeraFsException e) {
            throw new CacheException(CacheException.RESOURCE,
                                     String.format("Could not load cache "
                                                   + "locations for %s.",
                                                   location),
                            e);
        }
    }

    private void getResult(Connection connection,
                           String query,
                           String location,
                           Map<String, Integer> results)
                    throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(query);
            statement.setString(1, location);
            resultSet = statement.executeQuery();
            boolean addCounts = resultSet.getMetaData().getColumnCount() == 2;
            while (resultSet.next()) {
                String pnfsId = resultSet.getString(1);
                Integer count = null;
                if (addCounts) {
                    count = resultSet.getInt(2);
                }
                results.put(pnfsId, count);
            }
        } finally {
            SqlHelper.tryToClose(resultSet);
            SqlHelper.tryToClose(statement);
        }
    }

    private void validateInequality(String expression) throws ParseException {
        String validate = expression.trim();

        if (validate.length() < 1) {
            throw new ParseException(expression + " not a valid inequality.", 0);
        }

        switch (validate.charAt(0)) {
            case '=':
            case '<':
            case '>':
                validate = validate.substring(1).trim();
                break;
            default:
                throw new ParseException(expression
                                + " not a valid inequality.", 0);
        }

        if (validate.length() < 1) {
            throw new ParseException(expression + " not a valid inequality.", 1);
        }

        if (validate.charAt(0) == '=') {
            validate = validate.substring(1).trim();
        }

        if (validate.length() < 1) {
            throw new ParseException(expression + " not a valid inequality.", 2);
        }

        try {
            Integer.parseInt(validate);
        } catch (NumberFormatException e) {
            throw new ParseException(expression + " is not an integer.", 2);
        }
    }
}
