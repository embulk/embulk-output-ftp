package org.embulk.output.ftp;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import it.sauronsoftware.ftp4j.FTPClient;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.ftp.FtpFileOutputPlugin.PluginTask;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FileOutputRunner;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.standards.CsvParserPlugin;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.ssl.SSLPlugins;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TestFtpFileOutputPlugin
{
    private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = FtpFileOutputPlugin.CONFIG_MAPPER_FACTORY;
    private static final ConfigMapper CONFIG_MAPPER = FtpFileOutputPlugin.CONFIG_MAPPER;

    private static String FTP_TEST_HOST;
    private static Integer FTP_TEST_PORT;
    private static Integer FTP_TEST_SSL_PORT;
    private static String FTP_TEST_USER;
    private static String FTP_TEST_PASSWORD;
    private static String FTP_TEST_SSL_TRUSTED_CA_CERT_FILE;
    private static String FTP_TEST_SSL_TRUSTED_CA_CERT_DATA;
    private static String FTP_TEST_DIRECTORY;
    private static String FTP_TEST_PATH_PREFIX;
    private static String LOCAL_PATH_PREFIX;
    private FileOutputRunner runner;

    /*
     * This test case requires environment variables
     *   FTP_TEST_HOST
     *   FTP_TEST_USER
     *   FTP_TEST_PASSWORD
     *   FTP_TEST_SSL_TRUSTED_CA_CERT_FILE
     */
    @BeforeClass
    public static void initializeConstant() throws Exception
    {
        final Map<String, String> env = System.getenv();
        FTP_TEST_HOST = env.getOrDefault("FTP_TEST_HOST", "localhost");
        FTP_TEST_PORT = Integer.valueOf(env.getOrDefault("FTP_TEST_PORT", "11021"));
        FTP_TEST_SSL_PORT = Integer.valueOf(env.getOrDefault("FTP_TEST_SSL_PORT", "990"));
        FTP_TEST_USER = env.getOrDefault("FTP_TEST_USER", "scott");
        FTP_TEST_PASSWORD = env.getOrDefault("FTP_TEST_PASSWORD", "tiger");
        FTP_TEST_SSL_TRUSTED_CA_CERT_FILE = env.getOrDefault("FTP_TEST_SSL_TRUSTED_CA_CERT_FILE",
                "./docker-ftps/ca.cert.pem");
        FTP_TEST_SSL_TRUSTED_CA_CERT_DATA = env.getOrDefault("FTP_TEST_SSL_TRUSTED_CA_CERT_DATA",
                getFileContents("./docker-ftps/ca.cert.pem"));

        FTP_TEST_DIRECTORY = getDirectory(env.getOrDefault("FTP_TEST_DIRECTORY", "/unittest/"));
        FTP_TEST_PATH_PREFIX = FTP_TEST_DIRECTORY + "sample_";
        LOCAL_PATH_PREFIX = Resources.getResource("sample_01.csv").getPath();
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private FtpFileOutputPlugin plugin;

    @Before
    public void createResources() throws GeneralSecurityException, NoSuchMethodException, IOException
    {
        plugin = new FtpFileOutputPlugin();
        runner = new FileOutputRunner(runtime.getInstance(FtpFileOutputPlugin.class));
    }

    @Test
    public void checkDefaultValues()
    {
        final ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "ftp")
                .set("host", FTP_TEST_HOST)
                .set("user", FTP_TEST_USER)
                .set("password", FTP_TEST_PASSWORD)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        final PluginTask task = CONFIG_MAPPER.map(config, PluginTask.class);

        assertEquals(FTP_TEST_HOST, task.getHost());
        assertEquals(FTP_TEST_USER, task.getUser().get());
        assertEquals(FTP_TEST_PASSWORD, task.getPassword().get());
        assertEquals(true, task.getPassiveMode());
        assertEquals(false, task.getAsciiMode());
        assertEquals(false, task.getSsl());
        assertEquals(true, task.getSslExplicit());
        assertEquals(10, task.getMaxConnectionRetry());
        assertEquals("/", task.getDirectorySeparator());
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "ftp")
                .set("host", FTP_TEST_HOST)
                .set("port", FTP_TEST_PORT)
                .set("user", FTP_TEST_USER)
                .set("password", FTP_TEST_PASSWORD)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }

    @Test(expected = ConfigException.class)
    public void testTransactionWithInvalidHost()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "ftp")
                .set("host", "non-exists.example.com")
                .set("user", FTP_TEST_USER)
                .set("password", FTP_TEST_PASSWORD)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("max_connection_retry", 3)
                .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }

    @Test
    public void testTransactionWithUnverifiedSsl()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "ftp")
                .set("host", FTP_TEST_HOST)
                .set("port", FTP_TEST_SSL_PORT)
                .set("user", FTP_TEST_USER)
                .set("password", FTP_TEST_PASSWORD)
                .set("ssl", true)
                .set("ssl_verify", false)
                .set("ssl_verify_hostname", false)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }

    @Test
    public void testTransactionWithVerifiedSslWithCertFilePath()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "ftp")
                .set("host", FTP_TEST_HOST)
                .set("port", FTP_TEST_SSL_PORT)
                .set("user", FTP_TEST_USER)
                .set("password", FTP_TEST_PASSWORD)
                .set("ssl", true)
                .set("ssl_verify", true)
                .set("ssl_verify_hostname", false)
                .set("ssl_trusted_ca_cert_file", FTP_TEST_SSL_TRUSTED_CA_CERT_FILE)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }

    @Test
    public void testTransactionWithVerifiedSslWithCertFileData()
    {
        ConfigSource config = Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "ftp")
                .set("host", FTP_TEST_HOST)
                .set("port", FTP_TEST_SSL_PORT)
                .set("user", FTP_TEST_USER)
                .set("password", FTP_TEST_PASSWORD)
                .set("ssl", true)
                .set("ssl_verify", true)
                .set("ssl_verify_hostname", false)
                .set("ssl_trusted_ca_cert_data", FTP_TEST_SSL_TRUSTED_CA_CERT_DATA)
                .set("path_prefix", "my-prefix")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());

        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();

        runner.transaction(config, schema, 0, new Control());
    }

    @Test
    public void testResume()
    {
        final LegacyPluginTask taskLegacy = configLegacy().loadConfig(LegacyPluginTask.class);
        ConfigDiff configDiff = plugin.resume(taskLegacy.dump(), 0, new FileOutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
        //assertEquals("in/aa/a", configDiff.get(String.class, "last_path"));
    }

    @Test
    public void testCleanup()
    {
        final LegacyPluginTask taskLegacy = configLegacy().loadConfig(LegacyPluginTask.class);
        plugin.cleanup(taskLegacy.dump(), 0, Lists.<TaskReport>newArrayList()); // no errors happens
    }

    @Test
    public void testFtpFileOutputByOpen() throws Exception
    {
        final ConfigSource configSourceLegacy = configLegacy();
        final LegacyPluginTask taskLegacy = configSourceLegacy.loadConfig(LegacyPluginTask.class);
        final ConfigSource configSource = config();
        final PluginTask task = CONFIG_MAPPER.map(configSource, PluginTask.class);
        taskLegacy.setSSLConfig(SSLPlugins.configure(task));
        Schema schema = configSourceLegacy.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        runner.transaction(configSourceLegacy, schema, 0, new Control());

        TransactionalFileOutput output = plugin.open(taskLegacy.dump(), 0);

        output.nextFile();

        FileInputStream is = new FileInputStream(LOCAL_PATH_PREFIX);
        byte[] bytes = convertInputStreamToByte(is);
        Buffer buffer = Buffer.wrap(bytes);
        output.add(buffer);

        output.finish();
        output.commit();

        String remotePath = FTP_TEST_PATH_PREFIX + String.format(task.getSequenceFormat(), 0, 0) + task.getFileNameExtension();
        assertRecords(remotePath, task);
    }

    public ConfigSource configLegacy()
    {
        return Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "ftp")
                .set("host", FTP_TEST_HOST)
                .set("port", FTP_TEST_PORT)
                .set("user", FTP_TEST_USER)
                .set("password", FTP_TEST_PASSWORD)
                .set("path_prefix", FTP_TEST_PATH_PREFIX)
                .set("last_path", "")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());
    }

    public ConfigSource config()
    {
        return CONFIG_MAPPER_FACTORY.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "ftp")
                .set("host", FTP_TEST_HOST)
                .set("port", FTP_TEST_PORT)
                .set("user", FTP_TEST_USER)
                .set("password", FTP_TEST_PASSWORD)
                .set("path_prefix", FTP_TEST_PATH_PREFIX)
                .set("last_path", "")
                .set("file_ext", ".csv")
                .set("formatter", formatterConfig());
    }

    private class Control implements OutputPlugin.Control
    {
        @Override
        public List<TaskReport> run(TaskSource taskSource)
        {
            return Lists.newArrayList(Exec.newTaskReport());
        }
    }

    private ImmutableMap<String, Object> inputConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", LOCAL_PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        builder.add(ImmutableMap.of("name", "json_column", "type", "json"));
        return builder.build();
    }

    private ImmutableMap<String, Object> formatterConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("header_line", "false");
        builder.put("timezone", "Asia/Tokyo");
        return builder.build();
    }

    private void assertRecords(String remotePath, PluginTask task) throws Exception
    {
        ImmutableList<List<String>> records = getFileContentsFromFtp(remotePath, task);
        assertEquals(6, records.size());
        {
            List<String> record = records.get(1);
            assertEquals("1", record.get(0));
            assertEquals("32864", record.get(1));
            assertEquals("2015-01-27 19:23:49", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk", record.get(4));
            assertEquals("{\"k\":true}", record.get(5));
        }

        {
            List<String> record = records.get(2);
            assertEquals("2", record.get(0));
            assertEquals("14824", record.get(1));
            assertEquals("2015-01-27 19:01:23", record.get(2));
            assertEquals("20150127", record.get(3));
            assertEquals("embulk jruby", record.get(4));
            assertEquals("{\"k\":1}", record.get(5));
        }

        {
            List<String> record = records.get(3);
            assertEquals("{\"k\":1.23}", record.get(5));
        }

        {
            List<String> record = records.get(4);
            assertEquals("{\"k\":\"v\"}", record.get(5));
        }

        {
            List<String> record = records.get(5);
            assertEquals("{\"k\":\"2015-02-03 08:13:45\"}", record.get(5));
        }
    }

    private ImmutableList<List<String>> getFileContentsFromFtp(String path, PluginTask task) throws Exception
    {
        Method method = FtpFileOutputPlugin.class.getDeclaredMethod("newFTPClient", Logger.class, PluginTask.class);
        method.setAccessible(true);
        FTPClient client = (FTPClient) method.invoke(plugin, Exec.getLogger(FtpFileOutputPlugin.class), task);

        ImmutableList.Builder<List<String>> builder = new ImmutableList.Builder<>();
        File localFile = Exec.getTempFileSpace().createTempFile();
        client.download(path, localFile);
        InputStream is = new BufferedInputStream(new FileInputStream(localFile));
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line;
        while ((line = reader.readLine()) != null) {
            List<String> records = Arrays.asList(line.split(",", 0));

            builder.add(records);
        }
        return builder.build();
    }

    private static String getDirectory(String dir)
    {
        if (!dir.isEmpty() && !dir.endsWith("/")) {
            dir = dir + "/";
        }
        if (dir.startsWith("/")) {
            dir = dir.replaceFirst("/", "");
        }
        return dir;
    }

    private byte[] convertInputStreamToByte(InputStream is) throws IOException
    {
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        byte [] buffer = new byte[1024];
        while (true) {
            int len = is.read(buffer);
            if (len < 0) {
                break;
            }
            bo.write(buffer, 0, len);
        }
        return bo.toByteArray();
    }

    private static String getFileContents(String path) throws Exception
    {
        StringBuilder sb = new StringBuilder();
        try (InputStream is = new FileInputStream(new File(path))) {
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line = br.readLine();

            while (line != null) {
                sb.append(line).append("\n");
                line = br.readLine();
            }
        }
        return sb.toString();
    }

    private interface LegacyPluginTask extends org.embulk.config.Task
    {
        @org.embulk.config.Config("host")
        String getHost();

        @org.embulk.config.Config("port")
        @org.embulk.config.ConfigDefault("null")
        Optional<Integer> getPort();
        void setPort(Optional<Integer> port);

        @org.embulk.config.Config("user")
        @org.embulk.config.ConfigDefault("null")
        Optional<String> getUser();

        @org.embulk.config.Config("password")
        @org.embulk.config.ConfigDefault("null")
        Optional<String> getPassword();

        @org.embulk.config.Config("passive_mode")
        @org.embulk.config.ConfigDefault("true")
        boolean getPassiveMode();

        @org.embulk.config.Config("ascii_mode")
        @org.embulk.config.ConfigDefault("false")
        boolean getAsciiMode();

        @org.embulk.config.Config("ssl")
        @org.embulk.config.ConfigDefault("false")
        boolean getSsl();

        @org.embulk.config.Config("ssl_explicit")
        @org.embulk.config.ConfigDefault("true")
        boolean getSslExplicit();

        SSLPlugins.SSLPluginConfig getSSLConfig();
        void setSSLConfig(SSLPlugins.SSLPluginConfig config);

        @org.embulk.config.Config("path_prefix")
        String getPathPrefix();

        @org.embulk.config.Config("file_ext")
        String getFileNameExtension();

        @org.embulk.config.Config("sequence_format")
        @org.embulk.config.ConfigDefault("\"%03d.%02d\"")
        String getSequenceFormat();

        @org.embulk.config.Config("max_connection_retry")
        @org.embulk.config.ConfigDefault("10") // 10 times retry to connect FTP server if failed.
        int getMaxConnectionRetry();

        @org.embulk.config.Config("directory_separator")
        @org.embulk.config.ConfigDefault("\"/\"")
        String getDirectorySeparator();

        // Came from org.embulk.util.ftp.SSLPlugins.SSLPluginTask
        @org.embulk.config.Config("ssl_verify")
        @org.embulk.config.ConfigDefault("null")
        Optional<Boolean> getSslVerify();

        // Came from org.embulk.util.ftp.SSLPlugins.SSLPluginTask
        @org.embulk.config.Config("ssl_verify_hostname")
        @org.embulk.config.ConfigDefault("true")
        boolean getSslVerifyHostname();

        // Came from org.embulk.util.ftp.SSLPlugins.SSLPluginTask
        @org.embulk.config.Config("ssl_trusted_ca_cert_file")
        @org.embulk.config.ConfigDefault("null")
        Optional<String> getSslTrustedCaCertFile();

        // Came from org.embulk.util.ftp.SSLPlugins.SSLPluginTask
        @org.embulk.config.Config("ssl_trusted_ca_cert_data")
        @org.embulk.config.ConfigDefault("null")
        Optional<String> getSslTrustedCaCertData();
    }
}
