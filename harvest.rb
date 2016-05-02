require 'rubygems'
require 'prawn'
require 'fileutils'
require 'nokogiri'
require 'httparty'
require 'open-uri'
require File.join(File.dirname(__FILE__), 'cache')
require 'pp'
require 'thread'

Prawn::Font::AFM.hide_m17n_warning = true

require File.expand_path(File.dirname(__FILE__), 'dezoomify')

TARGET_DIR = "/Volumes/SDG/digibach/sources"
FileUtils.mkdir_p(TARGET_DIR) rescue nil

DERIVATE_URL_PATTERN = "http://www.bach-digital.de/servlets/MCRMETSServlet/%s?XSL.Style=dfg-source"

require 'net/http/persistent'
$http_connections = {}
def http_conn
  $http_connections[Thread.current] ||= 
    Net::HTTP::Persistent.new('crawl').tap do |h|
      h.read_timeout = 30
      h.open_timeout = 30
    end
end

class String
  def uri_escape
    gsub(/([^ a-zA-Z0-9_.-]+)/n) {'%'+$1.unpack('H2'*$1.size).
      join('%').upcase}.tr(' ', '+')
  end
  
  def uri_unescape
    tr('+', ' ').gsub(/((?:%[0-9a-fA-F]{2})+)/n){[$1.delete('%')].pack('H*')}
  end
  
  def safe_uri_escape
    gsub(/([^\/]+)$/) {$1.uri_escape}
  end
  
  def safe_dir
    gsub(/\s/, '').gsub(/[:\/]/, '-')
  end

  def safe_fn
    gsub(/[:\/]/, '-')
  end
end

class Harvester
  NICE_WORK_URL_PATTERN = "http://www.bach-digital.de/receive/%s"
  
  def initialize(work, url, bwvs)
    @work = work
    @url = url
    @nice_url = (@url =~ /^(.+)\?/) && $1
    @bwvs = bwvs
    @h = Cache.cached_xml(@url)
  end
  
  def page_number_for_sorting(page_ref)
    if page_ref =~ /(\d+)([a-z]+)/
      $1.to_i + ($2.bytes[0] - 'a'.bytes[0] + 1).to_f / 25
    else
      page_ref.to_i
    end
  end
  
  def derivate_links
    @derivate_links ||= (@h/:derobject).select {|n| n['title'] =~ /zoomify/}.
      sort_by {|n| (n['title'] =~ /^([0-9a-z]+) zoomify$/) && page_number_for_sorting($1)}.
      map {|n| DERIVATE_URL_PATTERN % n['href'] }
  end
  
  def derivate_docs
    @derivate_docs ||= 
      derivate_links.map {|l| doc = Cache.cached_xml(l) rescue nil}.compact
  end
  
  def title
    @title ||= (@h/:mcrstructure)[0]['classmark']
  end
  
  def metadata
    unless @meta
      @meta = {}
      @meta['work'] = @work
      @meta['bwvs'] = @bwvs
      @meta['url'] = @url
      @meta['title'] = (@h/:mcrstructure)[0]['classmark']
    end
    @meta
  end
  
  def derivate_info
    derivate_links.inject([]) {|m, l| m << l}
  end
  
  # Creates a list of pages from the derivate docs (a derivate doc is provided for each movement or section in the work)
  def page_refs
    last_page = ''
    @page_hrefs ||= derivate_docs.inject([]) do |pages, doc|
      derivate_url = DERIVATE_URL_PATTERN % doc.at('amdSec')['ID']
      derivate_url.gsub!('amd_', '')

      page_elements = (doc/"structMap[TYPE='PHYSICAL']//div[TYPE='page']").
        sort_by {|p| p['ORDER'].to_i }
        
      page_ids = page_elements.map {|p| p.at('fptr[FILEID^="MAX"]')['FILEID'] }

      page_ids.each do |id|
        page_url = doc.at("file[ID='#{id}']/FLocat")['href']
        pages << {
          page_id:      id,
          page_url:     page_url,
          derivate_url: derivate_url
        }
      end

      pages
    end
  end
  
  def save_info
    info = metadata.merge('hrefs' => derivate_info)
    File.open(File.join(TARGET_DIR, "#{title.safe_fn}.yml"), 'w+') {|f| f << info.to_yaml}
  end
  
  def process_pages
    refs = page_refs
    STDOUT << "found #{refs.size} pages"
    refs.each do |info|
      info[:jpg] = dezoomify_jpg(info[:page_url])
      yield info
    end
  end
  
  def pdf_filename
    "#{TARGET_DIR}/#{title.safe_fn}.pdf"
  end
  
  def format_link(text, href)
    "<link href='#{href}'><u>#{text}</u></link>"
  end
  
  def make_pdf
    return if derivate_docs.empty?
    
    t1 = Time.now
    
    pdf = Prawn::Document.new(:page_size => 'A4'); first = true
    pdf.font "Helvetica"
    pdf.font_size = 9
    
    work_refs = (@h/"source32s[class='MCRMetaLinkID']//[type='locator']")
    
    work_links = work_refs.map {|r| format_link(r['title'], NICE_WORK_URL_PATTERN % r['href'])}
    if work_links.size > 5
      work_links = work_links[0..4]
      work_links << "..."
    end
    
    header1 = "%s (%s)" % [
      format_link(title, @nice_url),
      work_links.join(", ")
    ]
    
    header1.encode!('Windows-1252', invalid: :replace, undef: :replace, replace: '?')
    text_opts = {inline_format: true, font_size: 7, align: :center}
    
    page = 0
    first = true
    
    process_pages do |info|
      pdf.start_new_page unless first; first = false;
      page += 1
      # page identification
      
      header2 = "page #{page} - #{format_link('derivate', info[:derivate_url])}"
      header2.encode!('Windows-1252', invalid: :replace, undef: :replace, replace: '?')
      
      pdf.text header1, text_opts
      pdf.text header2, text_opts.merge(font_size: 5)
      if info[:jpg]
        begin
          pdf.image StringIO.new(info[:jpg]), position: :center, vposition: :center, 
            fit: [520, 700]
        rescue
          pdf.text "Failed to load jpg", text_opts
        end
      else
        pdf.text "no jpg for page", text_opts
      end
    end
    STDOUT << "* #{page} pages (#{Time.now - t1}s) "
    t1 = Time.now
    pdf.render_file(pdf_filename) 
    puts "* render (#{Time.now - t1}s)"
  end
  
  def self.get_receipts
    if File.file?(File.join(TARGET_DIR, "receipts.yml"))
      YAML.load(IO.read(File.join(TARGET_DIR, "receipts.yml")))
    else
      {}
    end
  end
  
  def self.set_receipts(r)
    File.open(File.join(TARGET_DIR, "receipts.yml"), 'w+') {|f| f << r.to_yaml}
  end
  
  def self.already_processed?(href, name)
    exclusive do
      get_receipts[href] && File.file?("#{TARGET_DIR}/#{name.safe_fn}.pdf")
    end
  end
  
  def self.record_receipt(href)
    exclusive do
      set_receipts(get_receipts.merge(href => true))
    end
  end
  
  def self.format_bwv_dir_name(bwv)
    case bwv
    when /^(\d+)(.*)$/
      "BWV%04d%s" % [$1.to_i, $2]
    when /^Anh/
      "BWV#{bwv}"
    else
      bwv
    end
  end
  
  def self.process(entry)
    bwvs = entry.map {|i| i['BWV']}.uniq
    href = entry[0]['href']
    name = entry[0]['name']
    
    return if already_processed?(href, name)

    works = bwvs.map {|b| Harvester.format_bwv_dir_name(b)}.join(', ')
    puts "processing #{works}: #{name}"

    if bwvs.size > 1
      range = "%s-%s" % [format_bwv_dir_name(bwvs[0]).safe_dir, format_bwv_dir_name(bwvs[-1]).safe_dir]
    else
      entry = entry[0]
      work = format_bwv_dir_name(entry['BWV'])
    end
    
    FileUtils.mkdir(TARGET_DIR) rescue nil

    m = new(work, href, bwvs)
    m.save_info
    m.make_pdf
    record_receipt(href)

  rescue => e
    puts "Failed to process source for #{work}: #{e.message}"
    e.backtrace.each {|l| puts l}
  end
  
  SEMAPHORE = Mutex.new
  
  def self.exclusive(&block)
    SEMAPHORE.synchronize(&block)
  end
end

trap('INT') {exit}
trap('TERM') {exit}

$sources = YAML.load(IO.read('sources.yml'))
manuscripts = $sources.inject({}) do |m, w|
  href = w['href']
  (m[href] ||= []) << w
  m
end

idx = 1

manuscripts.each do |h, m|
  # works = entry.map {|i| Harvester.format_bwv_dir_name(i['BWV'])}.join(',')
  # next unless works.include?('BWV1069')
  Harvester.process(m)
  idx += 1
end

