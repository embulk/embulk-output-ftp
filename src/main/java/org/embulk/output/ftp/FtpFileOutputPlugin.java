package org.embulk.output.ftp;

import it.sauronsoftware.ftp4j.FTPAbortedException;
import it.sauronsoftware.ftp4j.FTPClient;
import it.sauronsoftware.ftp4j.FTPCommunicationListener;
import it.sauronsoftware.ftp4j.FTPConnector;
import it.sauronsoftware.ftp4j.FTPDataTransferException;
import it.sauronsoftware.ftp4j.FTPDataTransferListener;
import it.sauronsoftware.ftp4j.FTPException;
import it.sauronsoftware.ftp4j.FTPIllegalReplyException;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.config.UserDataException;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.TaskMapper;
import org.embulk.util.retryhelper.RetryExecutor;
import org.embulk.util.retryhelper.RetryGiveupException;
import org.embulk.util.retryhelper.Retryable;
import org.embulk.util.ssl.SSLPlugins;
import org.embulk.util.ssl.SSLPlugins.SSLPluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public class FtpFileOutputPlugin implements FileOutputPlugin
{
    public interface PluginTask extends Task, SSLPlugins.SSLPluginTask
    {
        @Config("host")
        String getHost();

        @Config("port")
        @ConfigDefault("null")
        Optional<Integer> getPort();
        void setPort(Optional<Integer> port);

        @Config("user")
        @ConfigDefault("null")
        Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        Optional<String> getPassword();

        @Config("passive_mode")
        @ConfigDefault("true")
        boolean getPassiveMode();

        @Config("ascii_mode")
        @ConfigDefault("false")
        boolean getAsciiMode();

        @Config("ssl")
        @ConfigDefault("false")
        boolean getSsl();

        @Config("ssl_explicit")
        @ConfigDefault("true")
        boolean getSslExplicit();

        SSLPluginConfig getSSLConfig();
        void setSSLConfig(SSLPluginConfig config);

        @Config("path_prefix")
        String getPathPrefix();

        @Config("file_ext")
        String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\"%03d.%02d\"")
        String getSequenceFormat();

        @Config("max_connection_retry")
        @ConfigDefault("10") // 10 times retry to connect FTP server if failed.
        int getMaxConnectionRetry();

        @Config("directory_separator")
        @ConfigDefault("\"/\"")
        String getDirectorySeparator();
    }

    static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules().build();
    static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();
    static final TaskMapper TASK_MAPPER = CONFIG_MAPPER_FACTORY.createTaskMapper();

    private static final Logger log = LoggerFactory.getLogger(FtpFileOutputPlugin.class);
    private static final Integer FTP_DEFULAT_PORT = 21;
    private static final Integer FTPS_DEFAULT_PORT = 990;
    private static final Integer FTPES_DEFAULT_PORT = 21;
    private static final long TRANSFER_NOTICE_BYTES = 100 * 1024 * 1024;

    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount, FileOutputPlugin.Control control)
    {
        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);
        task.setSSLConfig(SSLPlugins.configure(task));

        // try to check if plugin could connect to FTP server
        FTPClient client = null;
        try {
            client = newFTPClient(log, task);
        }
        catch (Exception ex) {
            throw new ConfigException("Faild to connect to FTP server", ex);
        }
        finally {
            disconnectClient(client);
        }

        return resume(task.toTaskSource(), taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, FileOutputPlugin.Control control)
    {
        control.run(taskSource);

        return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalFileOutput open(TaskSource taskSource, final int taskIndex)
    {
        final PluginTask task = TASK_MAPPER.map(taskSource, PluginTask.class);

        FTPClient client = newFTPClient(log, task);
        return new FtpFileOutput(client, task, taskIndex);
    }

    public static class FtpFileOutput implements TransactionalFileOutput
    {
        private final FTPClient client;
        private final String pathPrefix;
        private final String sequenceFormat;
        private final String pathSuffix;
        private final int maxConnectionRetry;
        private final String separator;
        private BufferedOutputStream output = null;
        private int fileIndex;
        private File file;
        private String filePath;
        private String remoteDirectory;
        private int taskIndex;

        public FtpFileOutput(FTPClient client, PluginTask task, int taskIndex)
        {
            this.client = client;
            this.taskIndex = taskIndex;
            this.pathPrefix = task.getPathPrefix();
            this.sequenceFormat = task.getSequenceFormat();
            this.pathSuffix = task.getFileNameExtension();
            this.maxConnectionRetry = task.getMaxConnectionRetry();
            this.separator = task.getDirectorySeparator();
        }

        @Override
        public void nextFile()
        {
            closeFile();

            try {
                String suffix = pathSuffix;
                if (!suffix.startsWith(".")) {
                    suffix = "." + suffix;
                }
                filePath = pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + suffix;
                if (!filePath.startsWith(separator)) {
                    filePath = separator + filePath;
                }
                remoteDirectory = getRemoteDirectory(filePath, separator);
                file = Exec.getTempFileSpace().createTempFile("tmp");
                log.info("Writing local temporary file \"{}\"", file.getAbsolutePath());
                output = new BufferedOutputStream(new FileOutputStream(file));
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        private void closeFile()
        {
            if (output != null) {
                try {
                    output.close();
                    fileIndex++;
                }
                catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }

        @Override
        public void add(Buffer buffer)
        {
            try {
                output.write(buffer.array(), buffer.offset(), buffer.limit());
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            finally {
                buffer.release();
            }
        }

        @Override
        public void finish()
        {
            close();
            uploadFile();
            disconnectClient(client);
        }

        private Void uploadFile()
        {
            if (filePath != null) {
                try {
                    return RetryExecutor.builder()
                            .withRetryLimit(maxConnectionRetry)
                            .withInitialRetryWaitMillis(500)
                            .withMaxRetryWaitMillis(30 * 1000)
                            .build()
                            .runInterruptible(new Retryable<Void>() {
                                @Override
                                public Void call() throws FTPIllegalReplyException, FTPException, FTPDataTransferException,
                                                          FTPAbortedException, IOException, RetryGiveupException
                                {
                                    try {
                                        client.changeDirectory(remoteDirectory);
                                    }
                                    catch (FTPException e) {
                                        try {
                                            client.createDirectory(remoteDirectory);
                                        }
                                        catch (FTPException e1) {
                                            if (e1.getCode() == 550) {
                                                // Create directory operation failed
                                                throw new OperationDeniedException(e1);
                                            }
                                        }
                                    }
                                    try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(file))) {
                                        client.upload(filePath, in, 0L, 0L,
                                                new LoggingTransferListener(file.getAbsolutePath(), filePath, log, TRANSFER_NOTICE_BYTES)
                                        );
                                    }
                                    if (!file.delete()) {
                                        throw new ConfigException("Couldn't delete local file " + file.getAbsolutePath());
                                    }
                                    log.info("Deleted local temporary file \"{}\"", file.getAbsolutePath());
                                    return null;
                                }

                                @Override
                                public boolean isRetryableException(Exception exception)
                                {
                                    return true;
                                }

                                @Override
                                public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                                        throws RetryGiveupException
                                {
                                    if (exception instanceof ConfigException) {
                                        throw new RetryGiveupException(exception);
                                    }
                                    else if (exception instanceof OperationDeniedException) {
                                        throw new ConfigException(exception);
                                    }
                                    String message = String.format("FTP put request failed. Retrying %d/%d after %d seconds. Message: %s",
                                            retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                                    if (retryCount % 3 == 0) {
                                        log.warn(message, exception);
                                    }
                                    else {
                                        log.warn(message);
                                    }
                                }

                                @Override
                                public void onGiveup(Exception firstException, Exception lastException)
                                        throws RetryGiveupException
                                {
                                }
                            });
                }
                catch (RetryGiveupException ex) {
                    final Throwable cause = ex.getCause();
                    if (cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    }
                    else if (cause instanceof Error) {
                        throw (Error) cause;
                    }
                    throw new RuntimeException(cause);
                }
                catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            return null;
        }

        @Override
        public void close()
        {
            closeFile();
        }

        @Override
        public void abort() {}

        @Override
        public TaskReport commit()
        {
            return CONFIG_MAPPER_FACTORY.newTaskReport();
        }

        private String getRemoteDirectory(String filePath, String separator)
        {
            Path path = Paths.get(filePath);
            if (path.getParent() == null) {
                return separator;
            }
            String parent = path.getParent().toString();
            if (!parent.startsWith(separator)) {
                parent = separator + parent;
            }
            return parent;
        }

        public class OperationDeniedException extends RuntimeException implements UserDataException
        {
            protected OperationDeniedException()
            {
            }

            public OperationDeniedException(String message)
            {
                super(message);
            }

            public OperationDeniedException(Throwable cause)
            {
                super(cause);
            }
        }
    }

    private static FTPClient newFTPClient(Logger log, PluginTask task)
    {
        FTPClient client = new FTPClient();
        Integer defaultPort = FTP_DEFULAT_PORT;
        try {
            if (task.getSsl()) {
                client.setSSLSocketFactory(SSLPlugins.newSSLSocketFactory(task.getSSLConfig(), task.getHost()));
                if (task.getSslExplicit()) {
                    client.setSecurity(FTPClient.SECURITY_FTPES);
                    defaultPort = FTPES_DEFAULT_PORT;
                    log.info("Using FTPES(FTPS/explicit) mode");
                }
                else {
                    client.setSecurity(FTPClient.SECURITY_FTPS);
                    defaultPort = FTPS_DEFAULT_PORT;
                    log.info("Using FTPS(FTPS/implicit) mode");
                }
            }

            if (!task.getPort().isPresent()) {
                task.setPort(Optional.of(defaultPort));
            }

            client.addCommunicationListener(new LoggingCommunicationListner(log));

            // TODO configurable timeout parameters
            client.setAutoNoopTimeout(3000);

            FTPConnector con = client.getConnector();
            con.setConnectionTimeout(30);
            con.setReadTimeout(60);
            con.setCloseTimeout(60);

            // for commons-net client
            //client.setControlKeepAliveTimeout
            //client.setConnectTimeout
            //client.setSoTimeout
            //client.setDataTimeout
            //client.setAutodetectUTF8

            client = connect(client, task);

            if (task.getUser().isPresent()) {
                log.info("Logging in with user {}", task.getUser().get());
                client.login(task.getUser().get(), task.getPassword().orElse(""));
            }

            log.info("Using passive mode");
            client.setPassive(task.getPassiveMode());

            if (task.getAsciiMode()) {
                log.info("Using ASCII mode");
                client.setType(FTPClient.TYPE_TEXTUAL);
            }
            else {
                log.info("Using binary mode");
                client.setType(FTPClient.TYPE_BINARY);
            }

            if (client.isCompressionSupported()) {
                log.info("Using MODE Z compression");
                client.setCompressionEnabled(true);
            }

            FTPClient connected = client;
            client = null;
            return connected;
        }
        catch (FTPException ex) {
            log.info("FTP command failed: {}, {}", ex.getCode(), ex.getMessage());
            throw new RuntimeException(ex);
        }
        catch (FTPIllegalReplyException ex) {
            log.info("FTP protocol error");
            throw new RuntimeException(ex);
        }
        catch (IOException ex) {
            log.info("FTP network error: {}", ex);
            throw new RuntimeException(ex);
        }
        finally {
            disconnectClient(client);
        }
    }

    private static FTPClient connect(final FTPClient client, final PluginTask task) throws InterruptedIOException
    {
        try {
            return RetryExecutor.builder()
                    .withRetryLimit(task.getMaxConnectionRetry())
                    .withInitialRetryWaitMillis(500)
                    .withMaxRetryWaitMillis(30 * 1000)
                    .build()
                    .runInterruptible(new Retryable<FTPClient>() {
                        @Override
                        public FTPClient call()
                        {
                            try {
                                if (task.getPort().isPresent()) {
                                    client.connect(task.getHost(), task.getPort().get());
                                    log.info("Connecting to {}:{}", task.getHost(), task.getPort().get());
                                }
                                else {
                                    client.connect(task.getHost());
                                    log.info("Connecting to {}", task.getHost());
                                }
                            }
                            catch (FTPIllegalReplyException | FTPException | IOException ex) {
                                throw new RuntimeException(ex);
                            }
                            return client;
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            if (exception.getCause() != null) {
                                if (exception.getCause() instanceof ConnectException && exception.getMessage().contains("Connection refused")) {
                                    return false;
                                }
                            }
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryGiveupException
                        {
                            String message = String.format("FTP Put request failed. Retrying %d/%d after %d seconds. Message: %s: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getClass(), exception.getMessage());
                            if (retryCount % 3 == 0) {
                                log.warn(message, exception);
                            }
                            else {
                                log.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException) throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            throw new RuntimeException(ex);
        }
        catch (InterruptedException ex) {
            throw new InterruptedIOException();
        }
    }

    static void disconnectClient(FTPClient client)
    {
        if (client != null && client.isConnected()) {
            try {
                client.disconnect(false);
            }
            catch (FTPException | FTPIllegalReplyException | IOException ex) {
                // do nothing
            }
        }
    }

    private static class LoggingCommunicationListner implements FTPCommunicationListener
    {
        private final Logger log;

        public LoggingCommunicationListner(Logger log)
        {
            this.log = log;
        }

        public void received(String statement)
        {
            log.debug("< " + statement);
        }

        public void sent(String statement)
        {
            if (statement.startsWith("PASS")) {
                // don't show password
                return;
            }
            log.debug("> {}", statement);
        }
    }

    private static class LoggingTransferListener implements FTPDataTransferListener
    {
        private final String localPath;
        private final String remotePath;
        private final Logger log;
        private final long transferNoticeBytes;

        private long totalTransfer;
        private long nextTransferNotice;

        public LoggingTransferListener(String localPath, String remotePath, Logger log, long transferNoticeBytes)
        {
            this.localPath = localPath;
            this.remotePath = remotePath;
            this.log = log;
            this.transferNoticeBytes = transferNoticeBytes;
            this.nextTransferNotice = transferNoticeBytes;
        }

        public void started()
        {
            log.info("Transfer started. local path:\"{}\" remote path:\"{}\"", localPath, remotePath);
        }

        public void transferred(int length)
        {
            totalTransfer += length;
            if (totalTransfer > nextTransferNotice) {
                log.info("Transferred {} bytes", totalTransfer);
                nextTransferNotice = ((totalTransfer / transferNoticeBytes) + 1) * transferNoticeBytes;
            }
        }

        public void completed()
        {
            log.info("Transfer completed. remote path:\"{}\", size:{} bytes", remotePath, totalTransfer);
        }

        public void aborted()
        {
            log.info("Transfer aborted");
        }

        public void failed()
        {
            log.info("Transfer failed");
        }
    }
}
