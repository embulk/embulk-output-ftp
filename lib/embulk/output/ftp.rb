Embulk::JavaPlugin.register_output(
  "ftp", "org.embulk.output.ftp.FtpFileOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
