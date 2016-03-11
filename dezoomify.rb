#!/usr/bin/env ruby
# Dezoomify. See README.markdown.
# By Henrik Nyh <http://henrik.nyh.se> 2009-02-06 under the MIT License.
# Original code here: https://gist.github.com/henrik/59636

# requirements:
# brew install ghostscript
# brew install imagemagick

require 'open-uri'
require 'nokogiri'
require 'thread'
require 'fileutils'
require File.join(File.dirname(__FILE__), 'thread_pool')
require File.join(File.dirname(__FILE__), 'cache')

ZOOMIFY_URL_PATTERN = "http://www.bach-digital.de/servlets/MCRZipFileNodeServlet/%s"
BASE_URL = "http://www.bach-digital.de"

$dezoomify_pool = ThreadPool.new(10)

def dezoomify_jpg(url)
  unless url =~ /MCRDFGServlet\/[^\/]+\/(.+)\.zip$/
    return Cache.cached_download(url)
  end
    
  url = ZOOMIFY_URL_PATTERN % $1
  
  jpg_filename = Cache.url_cache_path(url)
  STDOUT << "."
  Cache.get(Cache.key(url)) do
    xml_url = "#{url}/ImageProperties.xml"
    doc = Cache.cached_xml(xml_url)
    props = doc.at('IMAGE_PROPERTIES')
    unless props
      raise "Failed to get image properties for #{xml_url}"
    end
  
    width = props[:WIDTH].to_i
    height = props[:HEIGHT].to_i
    tilesize = props[:TILESIZE].to_f

    tiles_wide = (width/tilesize).ceil
    tiles_high = (height/tilesize).ceil

    # Determine max zoom level.
    # Also determine tile_counts per zoom level, used to determine tile group.
    # With thanks to http://trac.openlayers.org/attachment/ticket/1285/zoomify.patch.
    zoom = 0
    w = width
    h = height
    tile_counts = []
    while w > tilesize || h > tilesize
      zoom += 1

      t_wide = (w / tilesize).ceil
      t_high = (h / tilesize).ceil
      tile_counts.unshift t_wide*t_high

      w = (w / 2.0).floor
      h = (h / 2.0).floor
    end
    tile_counts.unshift 1  # Zoom level 0 has a single tile.
    tile_count_before_level = tile_counts[0..-2].inject(0) {|sum, num| sum + num }
  
    tile_files = []
    
    tiles_high.times do |y|
      tiles_wide.times do |x|
        tile_group = ((x + y * tiles_wide + tile_count_before_level) / tilesize).floor
        tilename = '%s-%s-%s.jpg' % [zoom, x, y]
        tile_url = "#{url}/TileGroup#{tile_group}/#{tilename}"
        
        tile_files << Cache.url_cache_path(tile_url)
        $dezoomify_pool.process {Cache.cached_download(tile_url)}
      end
    end
    $dezoomify_pool.join

    FileUtils.rm_f('dezoomify.err') if File.file?('dezoomify.err')
    `montage #{tile_files.join(' ')} -geometry +0+0 -tile #{tiles_wide}x#{tiles_high} #{jpg_filename} 2>dezoomify.err`
    unless $?.success?
      puts "Failed to compose #{url}:"
      puts IO.read('dezoomify.err')
    end
  
    # delete tiles
    tile_files.each {|fn| FileUtils.rm(fn) rescue nil}

    IO.read(jpg_filename)
  end
  
rescue => e
  puts e.message
  puts e.backtrace.join("\n")
end
