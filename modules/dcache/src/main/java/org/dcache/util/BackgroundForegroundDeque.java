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
package org.dcache.util;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>Combines a map index with two separate deques, one for foreground
 * (higher priority) operations, the other for lower priority (background)
 * operations.</p>
 *
 * <p>Provides for moving an operation to the front or the back
 * (when, e.g., it changes state).</p>
 *
 * <p>The map uses the ConcurrentHashMap implementation to allow access
 * during normal traffic, in which writes are understood to be frequent.
 * </p>
 *
 * <p>Provides for synchronized iteration processing over the deques through
 * the processor interface.</p>
 *
 * Created by arossi on 11/09/2015.
 */
public final class BackgroundForegroundDeque<K, V extends Backgroundable> {
    private Map<K, V> map = new ConcurrentHashMap<>();
    private Deque<V> foreground = new ArrayDeque<>();
    private Deque<V> background = new ArrayDeque<>();

    public V get(K key) {
        return map.get(key);
    }

    public Iterator<V> getValueIterator() {
        return map.values().iterator();
    }

    public long foregroundSize() {
        synchronized (foreground) {
            return foreground.size();
        }
    }

    public long backgroundSize() {
        synchronized (background) {
            return background.size();
        }
    }

    public long size() {
        return map.size();
    }

    public boolean insert(K key, V value) {
        V present = map.get(key);

        if (present != null) {
            present.incrementCount();
            return false;
        }

        map.put(key, value);

        if (value.isBackground()) {
            synchronized (background) {
                background.addLast(value);
            }
        } else {
            synchronized (foreground) {
                foreground.addLast(value);
            }
        }

        return true;
    }

    public void moveToBack(V value) {
        if (value.isBackground()) {
            synchronized (background) {
                background.remove(value);
                background.addLast(value);
            }
        } else {
            synchronized (foreground) {
                foreground.remove(value);
                foreground.addLast(value);
            }
        }
    }

    public void moveToFront(V value) {
        if (value.isBackground()) {
            synchronized (background) {
                background.remove(value);
                background.addFirst(value);
            }
        } else {
            synchronized (foreground) {
                foreground.remove(value);
                foreground.addFirst(value);
            }
        }
    }

    /**
     * Scan the deque under synchronization, applying the interface
     * method.
     */
    public void processBackground(BackgroundForegroundProcessor processor) {
        synchronized (background) {
            processor.processBackground(background.iterator());
        }
    }

    /**
     * Scan the deque under synchronization, applying the interface
     * method.
     */
    public void processForeground(BackgroundForegroundProcessor processor) {
        synchronized (foreground) {
            processor.processForeground(foreground.iterator());
        }
    }

    public V remove(K key) {
        V value = map.remove(key);

        if (value == null) {
            return null;
        }

        if (value.isBackground()) {
            synchronized (background) {
                background.remove(value);
            }
        } else {
            synchronized (foreground) {
                foreground.remove(value);
            }
        }

        return value;
    }
}
