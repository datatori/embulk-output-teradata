Embulk::JavaPlugin.register_output(
  "teradata", "org.embulk.output.teradata.TeradataOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
