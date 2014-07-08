# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = 'nata2-client'
  spec.version       = '0.0.1'
  spec.authors       = ['studio3104']
  spec.email         = ['studio3104.com@gmail.com']
  spec.summary       = %q{post/get slow query logs to/from Nata server}
  spec.description   = %q{post/get slow query logs to/from Nata server}
  spec.homepage      = 'https://github.com/studio3104/nata-client'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_runtime_dependency 'mysql-slowquery-parser'
  spec.add_runtime_dependency 'activesupport'
  spec.add_runtime_dependency 'toml-rb'
  spec.add_runtime_dependency 'sqlite3'
  spec.add_runtime_dependency 'mysql2-cs-bind'
  spec.add_runtime_dependency 'net-ssh'
  spec.add_runtime_dependency 'thor'

  spec.add_development_dependency 'bundler', '~> 1.5'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'rspec'
end
