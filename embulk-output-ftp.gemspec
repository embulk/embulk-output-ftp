
Gem::Specification.new do |spec|
  spec.name          = "embulk-output-ftp"
  spec.version       = "0.1.0"
  spec.authors       = ["Satoshi Akama"]
  spec.summary       = %[FTP file output plugin for Embulk]
  spec.description   = %[Stores files on FTP.]
  spec.email         = ["satoshiakama@gmail.com"]
  spec.licenses      = ["Apache 2.0"]
  set this: spec.homepage      = "https://github.com/sakama/embulk-output-ftp"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
