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
package org.dcache.alarms.logback;

import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.net.SMTPAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import ch.qos.logback.core.spi.CyclicBufferTracker;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.slf4j.Marker;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.dcache.alarms.IAlarms;
import org.dcache.alarms.Severity;
import org.dcache.alarms.dao.ILogEntryDAO;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.alarms.dao.impl.DataNucleusLogEntryStore;
import org.dcache.db.AlarmEnabledDataSource;

/**
 * For server-side interception of log messages. Will store them to the LogEntry
 * store used by the dCache installation. If the storage plugin is file-based
 * (e.g., XML), the dCache alarm display service (webadmin) must be running on a
 * shared file-system with the logging server.
 *
 * @author arossi
 */
public class LogEntryAppender extends AppenderBase<ILoggingEvent> {
    private static final String EMPTY_XML_STORE =
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<entries></entries>\n";

    /**
     *  Custom (user-facing) definitions/filters.
     */
    private final Map<String, AlarmDefinition> definitions
        = Collections.synchronizedMap(new TreeMap<String, AlarmDefinition>());

    /**
     *  Priority mappings.
     */
    // TODO

    /**
     * The main configuration for storing alarms through DAO.
     */
    private ILogEntryDAO store;
    private File xmlPath;
    private String dnPropertiesPath;
    private String customDefinitionsPath;
    private String url;
    private String user;
    private String password;
    private JDOPersistenceManagerFactory pmf;

    /**
     * Obviously, we do not wrap this data source with the
     * {@link AlarmEnabledDataSource} because we don't want
     * internal errors being logged to the alarm system itself.
     */
    private HikariDataSource dataSource;

    /**
     * Optional email appender configuration.
     */
    private SMTPAppender emailAppender;
    private boolean emailEnabled;
    private Severity emailThreshold;
    private String emailEncoding;
    private String smtpHost;
    private int smtpPort;
    private boolean startTls;
    private boolean ssl;
    private String emailUser;
    private String emailPassword;
    private String[] emailRecipients;
    private String emailSender;
    private String emailSubject;
    private int emailBufferSize;

    /**
     * Optional history file appender configuration.
     */
    private FileAppender<ILoggingEvent> historyAppender;
    private boolean historyEnabled;
    private Severity historyThreshold;
    private String historyEncoding;
    private String historyFile;
    private String historyFileNamePattern;
    private String historyMaxFileSize;
    private int historyMinIndex;
    private int historyMaxIndex;

    public void addAlarmType(AlarmDefinition definition) {
        definitions.put(definition.getType(), definition);
    }

    public void setCustomDefinitionsPath(String customDefinitionsPath) {
        this.customDefinitionsPath = customDefinitionsPath;
    }

    public void setEmailBufferSize(int emailBufferSize) {
        this.emailBufferSize = emailBufferSize;
    }

    public void setEmailEnabled(boolean emailEnabled) {
        this.emailEnabled = emailEnabled;
    }

    public void setEmailEncoding(String emailEncoding) {
        this.emailEncoding = emailEncoding;
    }

    public void setEmailPassword(String emailPassword) {
        this.emailPassword = emailPassword;
    }

    public void setEmailRecipients(String emailRecipients) {
        Preconditions.checkNotNull(emailRecipients);
        this.emailRecipients = emailRecipients.split("[,]");
    }

    public void setEmailSender(String emailSender) {
        this.emailSender = emailSender;
    }

    public void setEmailSubject(String emailSubject) {
        this.emailSubject = emailSubject;
    }

    public void setEmailThreshold(String emailThreshold) {
        this.emailThreshold = Severity.valueOf(emailThreshold.toUpperCase());
    }

    public void setEmailUser(String emailUser) {
        this.emailUser = emailUser;
    }

    public void setHistoryEnabled(boolean historyEnabled) {
        this.historyEnabled = historyEnabled;
    }

    public void setHistoryEncoding(String historyEncoding) {
        this.historyEncoding = historyEncoding;
    }

    public void setHistoryFile(String historyFile) {
        this.historyFile = historyFile;
    }

    public void setHistoryFileNamePattern(String historyFileNamePattern) {
        this.historyFileNamePattern = historyFileNamePattern;
    }

    public void setHistoryMaxFileSize(String historyMaxFileSize) {
        this.historyMaxFileSize = historyMaxFileSize;
    }

    public void setHistoryMaxIndex(int historyMaxIndex) {
        this.historyMaxIndex = historyMaxIndex;
    }

    public void setHistoryMinIndex(int historyMinIndex) {
        this.historyMinIndex = historyMinIndex;
    }

    public void setHistoryThreshold(String historyThreshold) {
        this.historyThreshold = Severity.valueOf(historyThreshold.toUpperCase());
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setPmf(JDOPersistenceManagerFactory pmf) {
        this.pmf = pmf;
    }

    public void setDnPropertiesPath(String dnPropertiesPath) {
        this.dnPropertiesPath = dnPropertiesPath;
    }

    public void setSmtpHost(String smtpHost) {
        this.smtpHost = smtpHost;
    }

    public void setSmtpPort(int smtpPort) {
        this.smtpPort = smtpPort;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public void setStartTls(boolean startTls) {
        this.startTls = startTls;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setXmlPath(String xmlPath) {
        this.xmlPath = new File(xmlPath);
    }

    @Override
    public void start() {
        try {
            if (Strings.emptyToNull(customDefinitionsPath) != null) {
                File file = new File(customDefinitionsPath);
                if (!file.exists()) {
                    throw new FileNotFoundException(file.getAbsolutePath());
                }
                loadDefinitions(file);
            }

            if (store == null) {
                initPersistenceManagerFactory();
                store = new DataNucleusLogEntryStore(pmf);
            }

            if (emailEnabled) {
                startEmailAppender();
            }

            if (historyEnabled) {
                startHistoryAppender();
            }

            super.start();
        } catch ( JDOMException | IOException t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void stop()
    {
        super.stop();
        store = null;

        if (pmf != null) {
            pmf.close();
            pmf = null;
        }

        if (dataSource != null) {
            dataSource.shutdown();
            dataSource = null;
        }

        if (emailAppender != null) {
            emailAppender.stop();
            emailAppender = null;
        }

        if (historyAppender != null) {
            historyAppender.stop();
            historyAppender = null;
        }
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        if (isStarted()) {
            LogEntry entry = createEntryFromEvent(eventObject);
            String type = entry.getType();
            int priority = getPriority(type);

            /*
             * The history log parses out all messages above a certain priority.
             * This is just a convenience for sifting messages
             * from the normal domain log and recording them them using
             * the more specific alert pattern.
             */
            if (historyEnabled && priority >= historyThreshold.ordinal()) {
                historyAppender.doAppend(eventObject);
            }

            /*
             * Remote messages which are indeed alarms/alerts can be
             * sent as email.  Only these messages will be placed in
             *
             */
            if (emailEnabled &&
                            entry.isAlarm() &&
                            priority >= emailThreshold.ordinal()) {
                    emailAppender.doAppend(eventObject);
            }

            /*
             * Now store the remote event.
             */
            store.put(entry);
        }
    }

    /**
     * Largely a convenience for internal testing.
     */
    void setStore(ILogEntryDAO store) {
        this.store = store;
    }

    private LogEntry createEntryFromEvent(ILoggingEvent eventObject) {
        LogEntry entry = new LogEntry();
        Long timestamp = eventObject.getTimeStamp();
        entry.setFirstArrived(timestamp);
        entry.setLastUpdate(timestamp);
        entry.setInfo(eventObject.getFormattedMessage());
        Map<String, String> mdc = eventObject.getMDCPropertyMap();

        String host = mdc.get(IAlarms.HOST_TAG);
        if (host == null) {
            host = IAlarms.UNKNOWN_HOST;
        }

        String domain = mdc.get(IAlarms.DOMAIN_TAG);
        if (domain == null) {
            domain = IAlarms.UNKNOWN_DOMAIN;
        }

        String service = mdc.get(IAlarms.SERVICE_TAG);
        if (service == null) {
            service = IAlarms.UNKNOWN_SERVICE;
        }

        entry.setHost(host);
        entry.setDomain(domain);
        entry.setService(service);

        postProcessAlarm(eventObject, entry);
        return entry;
    }

    /**
     */
    private int getPriority(String type) {
        // TODO Priority map
        return Severity.CRITICAL.ordinal();
    }

    /**
     * Checks for the existence of the file and creates it if not. Note that
     * existing files are not validated against any schema, explicit or
     * implicit. If the parent does not exist, an exception will be thrown.
     */
    private void initializeXmlFile(File file) throws IOException {
        if (!file.exists()) {
            if (!file.getParentFile().isDirectory()) {
                String parent = file.getParentFile().getAbsolutePath();
                throw new FileNotFoundException(parent + " is not a directory");
            }
            Files.write(EMPTY_XML_STORE, file, Charsets.UTF_8);
        }
    }

    private void initPersistenceManagerFactory() throws IOException
    {
        Properties properties = new Properties();
        if (dnPropertiesPath != null && !dnPropertiesPath.trim().isEmpty()) {
            File file = new File(dnPropertiesPath);
            if (!file.exists()) {
                throw new FileNotFoundException(file.getAbsolutePath());
            }
            try (InputStream stream = new FileInputStream(file)) {
                properties.load(stream);
            }
        }

        if (url.startsWith("xml:")) {
            initializeXmlFile(xmlPath);
            properties.setProperty("datanucleus.ConnectionURL", url);
        } else if (url.startsWith("jdbc:")) {
            HikariConfig config = new HikariConfig();
            config.setDataSource(new DriverManagerDataSource(url, user, password));
            dataSource = new HikariDataSource(config);
            properties.setProperty("datanucleus.connectionPoolingType", "None");
        }

        pmf = new JDOPersistenceManagerFactory(
                Maps.<String, Object>newHashMap(Maps.fromProperties(properties)));
        if (dataSource != null) {
            pmf.setConnectionFactory(dataSource);
        }
    }

    private void loadDefinitions(File xmlFile) throws JDOMException,
                    IOException {
        SAXBuilder builder = new SAXBuilder();
        Document document = builder.build(xmlFile);
        Element rootNode = document.getRootElement();
        List<Element> list = rootNode.getChildren(AlarmDefinition.ALARM_TYPE_TAG);
        for (Element node: list) {
            addAlarmType(new AlarmDefinition(node));
        }
    }

    private void postProcessAlarm(ILoggingEvent event, LogEntry entry) {
        Marker marker = event.getMarker();
        boolean alarm = marker == null ? false
                        : marker.contains(IAlarms.ALARM_MARKER);

        AlarmDefinition match = null;

        if (!alarm) {
            for (AlarmDefinition definition : definitions.values()) {
                if (definition.matches(event)) {
                    alarm = true;
                    match = definition;
                    break;
                }
            }
        }

        entry.setAlarm(alarm);
        entry.setAlarmMetadata(event, match);
    }

    private void startEmailAppender() {
        PatternLayout layout = new PatternLayout();
        layout.setPattern(emailEncoding);
        layout.setContext(getContext());
        layout.start();

        CyclicBufferTracker bufferTracker = new CyclicBufferTracker();
        bufferTracker.setBufferSize(emailBufferSize);

        emailAppender = new SMTPAppender();
        emailAppender.setContext(context);
        emailAppender.setPassword(emailPassword);
        emailAppender.setUsername(emailUser);
        emailAppender.setSmtpHost(smtpHost);
        emailAppender.setSmtpPort(smtpPort);
        emailAppender.setSSL(ssl);
        emailAppender.setSTARTTLS(startTls);
        emailAppender.setFrom(emailSender);
        emailAppender.setSubject(emailSubject);
        for (String to: emailRecipients) {
            emailAppender.addTo(to);
        }
        emailAppender.setLayout(layout);
        emailAppender.setCyclicBufferTracker(bufferTracker);
        emailAppender.start();
    }

    private void startHistoryAppender() throws FileNotFoundException {
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setPattern(historyEncoding);
        encoder.setContext(getContext());
        encoder.start();

        RollingFileAppender<ILoggingEvent> historyAppender = new RollingFileAppender<>();
        FixedWindowRollingPolicy fwrp = new FixedWindowRollingPolicy();
        fwrp.setMaxIndex(historyMaxIndex);
        fwrp.setMinIndex(historyMinIndex);
        fwrp.setFileNamePattern(historyFileNamePattern);
        fwrp.setContext(context);

        SizeBasedTriggeringPolicy sbtp = new SizeBasedTriggeringPolicy();
        sbtp.setMaxFileSize(historyMaxFileSize);
        sbtp.setContext(context);

        historyAppender.setContext(context);
        historyAppender.setEncoder(encoder);
        historyAppender.setFile(historyFile);
        historyAppender.setTriggeringPolicy(sbtp);
        historyAppender.setRollingPolicy(fwrp);

        fwrp.setParent(historyAppender);
        fwrp.start();
        sbtp.start();
        historyAppender.start();

        this.historyAppender = historyAppender;
    }
}
