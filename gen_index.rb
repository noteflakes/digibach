# encoding: UTF-8

require 'rubygems'
require "bundler/setup"

require 'mustache'

Mustache.template_path = File.dirname(__FILE__)
Mustache.template_extension = 'html'

TARGET_DIR = "/Volumes/SCHATZ EXT/digibach"

$works = {}

def bwv_ref(source)
  bwv = source['BWV']
  
  if bwv =~ /^(BC|Serie)/ || bwv.empty?
    bwv
  else
    "BWV #{bwv}"
  end
end

def work_url(source)
  "http://www.bach-digital.de/receive/BachDigitalWork_work_%08d" % 
    source["work_id"]
end

def pdf_url(source)
  "sources/#{source['name'].gsub(/[:\/]/, '-')}.pdf"
end

YAML.load(IO.read('sources.yml')).each do |source|
  work = ($works[source['work_id']] ||= {'id' => source['work_id'], 
    'bwv' => bwv_ref(source), 'work_url' => work_url(source), 
    'title' => source["title"], 'sources' => []})
  
  work["sources"] << {
    'title' => source['name'],
    'pdf_path' => pdf_url(source)
  }
end

$works = $works.values.sort_by {|w| w['id']}

class IndexPage < Mustache
  self.template_name = 'index_template'
  
  def works
    $works
  end
end

File.open(File.join(TARGET_DIR, 'index.html'), "w") {|f| f << IndexPage.render}