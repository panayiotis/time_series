# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'version'

Gem::Specification.new do |spec|
  spec.name          = "time_series"
  spec.version       = Time_Series::VERSION
  spec.author        = 'Panayiotis Vlantis'
  spec.email         = 'p.vlantis@di.uoa.gr'
  spec.description   = "Timeseries metrics store using Redis as a backend"
  spec.summary       = "Timeseries metrics store using Redis as a backend"
  spec.homepage      = "https://github.com/panayiotis/time_series"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "minitest"

  spec.add_dependency 'awesome_print'
  spec.add_dependency 'activesupport'
  spec.add_dependency 'redis'
  spec.add_dependency 'hiredis'

end
