# http://http://www.bach-digital.de//receive/BachDigitalWork_work_00000001
# http://http://www.bach-digital.de//receive/BachDigitalWork_work_00000249

$work_range = 1..1727
#URL_PATTERN = "http://www.bach-digital.de/receive/BachDigitalWork_work_%08d"

WORK_URL_PATTERN = "http://www.bach-digital.de/receive/BachDigitalWork_work_%08d?XSL.Transformer=mei"
SOURCE_URL_PATTERN = "http://www.bach-digital.de/receive/%s?XSL.Style=structure"

require 'rubygems'
require 'thread'
require 'nokogiri'
require 'open-uri'
require 'httparty'
require 'yaml'
require File.join(File.dirname(__FILE__), 'thread_pool')

trap("INT") {exit}
trap("TERM") {exit}

$sources = (YAML.load(IO.read('sources.yml')) rescue nil) || []

# clean sources
$sources.select! {|s| s[:work].to_i == 0}

$start_sources = $sources.clone
$pool = ThreadPool.new(10)

require 'net/http/persistent'
$http_connections = {}
def http_conn
  $http_connections[Thread.current] ||=
    Net::HTTP::Persistent.new('crawl').tap do |h|
      h.read_timeout = 30
      h.open_timeout = 30
    end
end

def open_url(url)
  r = http_conn.request URI(url)
  r.body
  # r = HTTParty.get(url, :timeout => 30)
  # r.body
rescue Timeout::Error
  puts "\ntimeout: #{url}"
  ''
end

def open_xml(url)
  Nokogiri::XML(open_url(url))
end

def check_work(id)
  STDOUT << "*"
  url = WORK_URL_PATTERN % id
  h = open_xml(url)

  work_tag = (h/:work/:identifier)[0]
  unless work_tag
    raise "Could not find XML info: #{url}"
  end

  work = work_tag.content

  bwv_node = (h/:work/:identifier).select {|n| n.content =~ /BWV/}[0] ||
    (h/:work/:identifier)[0]

  bwv = case bwv_node.content
  when /BWV\s+(\d+[a-z]?)/
    $1
  when /BWV\s(Anh\.\s+\d+[a-z]?)/
    $1
  when /BC\s([A-Z]+\s(\w+))/
    "BC #{$1}"
  when /Emans \((\S+)\)/
    "Emans #{$1}"
  when /deest \((.+)\)/
    $1
  else
    bwv_node.content
  end

  title = (h/:title)[0].content

  sources = (h/:source).map do |n|
    {
      ref: (n/:title)[0].content,
      url: SOURCE_URL_PATTERN % n['xml:id']
    }
  end

  sources.each do |s|
    $pool.process {check_source(s[:url], s[:ref], id, work, title, bwv)}
  end
rescue => e
  puts "!"
  puts "\nError encountered while processing #{url}"
  puts e.message
  e.backtrace.each {|l| puts l}
end

SEMAPHORE = Mutex.new

def exclusive(&block)
  SEMAPHORE.synchronize(&block)
end

def check_source(url, name, work_id, work, title, bwv)
  STDOUT << "."
  source_already_marked = $sources.select {|s| s['href'] == url}.size > 0
  if source_already_marked || is_source_digitized?(url)
    unless source_already_marked
      puts "found source for BWV #{bwv}: #{name}"
    end
    exclusive do
      $sources << {
        'work_id' => work_id,
        'work' => work,
        'title' => title,
        'BWV' => bwv,
        'href' => url,
        'name' => name
      }
      save_sources
    end
  end
end

$digitized_map = {}

def is_source_digitized?(url)
  return $digitized_map[url] if $digitized_map.has_key?(url)

  h = open_xml(url)
  exclusive do
    $digitized_map[url] = (h/"mcritem[@type='source']"/:derobject).size > 0
  end
end

def save_sources
  File.open('sources.yml', 'w+') {|f| f << $sources.uniq.sort_by {|m| [m['work_id'], m['href']]}.to_yaml}
end

$work_range.each {|id| check_work(id)}
$pool.join
puts "*************************"
puts "found #{$sources.uniq.size} (#{$start_sources.uniq.size})"
