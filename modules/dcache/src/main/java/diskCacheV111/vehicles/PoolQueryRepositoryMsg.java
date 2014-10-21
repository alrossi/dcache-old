// $Id: PoolQueryRepositoryMsg.java,v 1.2 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;

import java.util.List;

import diskCacheV111.repository.CacheRepositoryEntryInfo;
import diskCacheV111.util.IteratorCookie;
public class PoolQueryRepositoryMsg extends PoolMessage {

   private IteratorCookie _cookie  = new IteratorCookie() ;
   private List<CacheRepositoryEntryInfo> _infos;
   private String _pnfsid;

    private static final long serialVersionUID = 5505604194473710945L;

   public PoolQueryRepositoryMsg(String poolName ){
      super(poolName) ;
      this.setReplyRequired(true);
   }
   public PoolQueryRepositoryMsg( String poolName , IteratorCookie cookie ){
      this(poolName);
      _cookie = cookie ;
   }
   public PoolQueryRepositoryMsg(String poolName, String pnfsid) {
       this(poolName);
       _pnfsid = pnfsid;
   }

   public void setReply( IteratorCookie cookie , List<CacheRepositoryEntryInfo> infos ){
      _cookie  = cookie ;
      _infos = infos ;
      setReply();
   }
   public List<CacheRepositoryEntryInfo> getInfos(){ return _infos ; }
   public IteratorCookie getCookie(){ return _cookie ; }
   public String getPnfsid(){ return _pnfsid; }
}
