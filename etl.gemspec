# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'etl/version'

Gem::Specification.new do |spec|
  spec.name          = "etl"
  spec.version       = ETL::VERSION
  spec.authors       = [""]
  spec.email         = [""]
  spec.summary       = %q{Export / Transform / Load}
  spec.description   = %q{To help with importing and transforming data from an external database}
  spec.homepage      = ""
  spec.license       = ""

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.13.6" # MIT License
  spec.add_development_dependency "rake", "~> 11.3.0" # MIT License
  spec.add_development_dependency "rspec" # MIT License
  spec.add_development_dependency "pry-rescue" # MIT License

  spec.add_runtime_dependency "activerecord", "~> 5.0.0.1" # MIT License
  spec.add_runtime_dependency "activerecord-import", "~> 0.16.1" # Ruby License
end
