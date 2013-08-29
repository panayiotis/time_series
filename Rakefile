#!/usr/bin/env rake
require "bundler/gem_tasks"

require 'rake/testtask'

`reset`

Rake::TestTask.new do |t|
  t.libs << 'lib/time_series'
  t.test_files = FileList['spec/*spec.rb']
  t.verbose = true
end
 
task :default => :test
