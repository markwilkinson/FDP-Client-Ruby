require "linkeddata"
require "rest-client"

module FDPMate
  DCAT = RDF::Vocabulary.new("http://www.w3.org/ns/dcat#")
  FOAF = RDF::Vocabulary.new("http://xmlns.com/foaf/0.1/")
  BS = RDF::Vocabulary.new("http://rdf.biosemantics.org/ontologies/fdp-o#")

  class DCATResource
    attr_accessor :baseURI, :parentURI, :serverURL, :accessRights,
                  :conformsTo, :contactName, :contactEmail, :creator,
                  :creatorName, :title, :description, :issued, :modified,
                  :hasVersion, :publisher, :identifier, :license, :language,
                  :dataset, :keyword, :landingPage, :qualifiedRelation,
                  :theme, :service, :themeTaxonomy, :homepage, :types, :g  # the graph

    def initialize(types: [DCAT.Resource], baseURI: nil, parentURI: nil,
                   accessRights: nil, conformsTo: nil, contactEmail: nil, contactName: nil, creator: nil, creatorName: nil,
                   title: nil, description: nil, issued: nil, modified: nil, hasVersion: nil, publisher: nil,
                   identifier: nil, license: nil, language: "http://id.loc.gov/vocabulary/iso639-1/en",
                   dataset: nil, keyword: nil, landingPage: nil, qualifiedRelation: nil, theme: nil,
                   service: nil, themeTaxonomy: nil, homepage: nil, serverURL: "http://localhost:7070",
                   **_args)

      @accessRights = accessRights
      @conformsTo = conformsTo
      @contactName = contactName
      @contactEmail = contactEmail
      @creator = creator
      @creatorName = creatorName
      @title = title
      @description = description
      @issued = issued
      @modified = modified
      @hasVersion = hasVersion
      @publisher = publisher
      @identifier = identifier
      @license = license
      @language = language

      @dataset = dataset
      @keyword = keyword
      @landingPage = landingPage
      @qualifiedRelation = qualifiedRelation
      @theme = theme
      @service = service # this is now defunct, I think
      @themeTaxonomy = themeTaxonomy
      @homepage = homepage

      @serverURL = RDF::URI(serverURL)
      @baseURI = RDF::URI(baseURI)
      @parentURI = RDF::URI(parentURI)
      @types = types

      abort "you must set baseURI and serverURL parameters" unless self.baseURI and self.serverURL

      set_headers
    end

    def set_headers
      return if $headers

      puts ENV.fetch("FDPUSER", nil)
      puts ENV.fetch("FDPPASS", nil)
      payload = '{ "email": "' + ENV.fetch("FDPUSER", nil) + '", "password": "' + ENV.fetch("FDPPASS", nil) + '" }'
      warn "#{serverURL}/tokens", payload
      resp = RestClient.post("#{serverURL}/tokens", payload, headers = { content_type: "application/json" })
      $token = JSON.parse(resp.body)["token"]
      puts $token
      $headers = { content_type: "text/turtle", authorization: "Bearer #{$token}", accept: "text/turtle" }
    end

    def build
      @g = RDF::Graph.new # reset graph
      abort "an identifier has not been set" unless identifier
      types.each do |type|
        g << [identifier, RDF.type, type]
      end

      g << [identifier, RDF::Vocab::RDFS.label, @title] if @title
      g << [identifier, RDF::Vocab::DC.isPartOf, @parentURI] if @parentURI

      # DCAT
      %w[landingPage qualifiedRelation themeTaxonomy endpointURL endpointDescription].each do |f|
        (pred, value) = get_pred_value(f, "DCAT")
        next unless pred and value

        g << [identifier, pred, value]
      end
      # DCAT Multi-value
      %w[keyword].each do |f|
        (pred, value) = get_pred_value(f, "DCAT") # value is a comma-separated string
        next unless pred and value

        keywords = value.split(",")
        keywords.each do |kw|
          kw.strip!
          next if kw.empty?

          g << [identifier, pred, kw]
        end
      end

      # DCT
      %w[accessRights hasVersion conformsTo title description identifier license language creator].each do |f|
        (pred, value) = get_pred_value(f, "DCT")
        next unless pred and value

        g << [identifier, pred, value]
      end
      %w[issued modified].each do |f|
        warn "doing issued modified #{f}"
        (pred, value) = get_pred_value(f, "DCT", "TIME")
        next unless pred and value

        g << [identifier, pred, value]
        g << [identifier, BS.issued, value]
        g << [identifier, BS.modified, value]
      end

      # FOAF
      %w[homepage].each do |f|
        (pred, value) = get_pred_value(f, "FOAF")
        next unless pred and value

        g << [identifier, pred, value]
      end

      # COMPLEX

      # identifier
      # contactPoint
      if contactEmail or contactName
        bnode = RDF::URI.new(identifier.to_s + "#contact")
        g << [identifier, DCAT.contactPoint, bnode]
        g << [bnode, RDF.type, RDF::URI.new("http://www.w3.org/2006/vcard/ns#Individual")]
        g << [bnode, RDF::URI.new("http://www.w3.org/2006/vcard/ns#fn"), contactName] if contactName
        g << [bnode, RDF::URI.new("http://www.w3.org/2006/vcard/ns#hasEmail"), contactEmail] if contactEmail
      end

      # publisher
      if publisher
        bnode = RDF::Node.new
        g << [identifier, RDF::Vocab::DC.publisher, bnode]
        g << [bnode, RDF.type, FOAF.Agent]
        g << [bnode, FOAF.name, publisher]
      end

      # creator
      if creator
        g << [identifier, RDF::Vocab::DC.creator, RDF::URI.new(creator)]
        g << [RDF::URI.new(creator), RDF.type, FOAF.Agent]
        g << [RDF::URI.new(creator), FOAF.name, creatorName] if creatorName
      end

      # accessRights
      if accessRights
        g << [identifier, RDF::Vocab::DC.accessRights, RDF::URI.new(accessRights)]
        g << [RDF::URI.new(accessRights), RDF.type, RDF::Vocab::DC.RightsStatement]
      end

      # dataService
      if is_a? DCATDataService
        warn inspect
        warn "serializing data service #{endpointDescription} or #{endpointURL}"
        if endpointDescription or endpointURL
          warn "serializing ENDPOINTS"
          bnode = RDF::Node.new
          g << [identifier, DCAT.accessService, bnode]
          g << [bnode, RDF.type, DCAT.dataService]
          if endpointDescription
            g << [bnode, DCAT.endpointDescription,
                  RDF::URI.new(endpointDescription)]
          end
          g << [bnode, DCAT.endpointURL, RDF::URI.new(endpointURL)] if endpointURL
        end
      end

      # mediaType or format  https://www.iana.org/assignments/media-types/application/3gppHalForms+json
      if is_a? DCATDistribution
        if mediaType
          # CHANGE THIS BACK WHEN FDP SHACL validation is correct
          # type = "https://www.iana.org/assignments/media-types/" + self.mediaType
          # type = RDF::URI.new(type)
          type = mediaType
          g << [identifier, DCAT.mediaType, type]
          # CHANGE THIS BACK ALSO!
          # self.g << [type, RDF.type, RDF::Vocab::DC.MediaType]
        end
        if self.format
          type = RDF::URI.new(self.format)
          g << [identifier, RDF::Vocab::DC.format, type]
          g << [type, RDF.type, RDF::Vocab::DC.MediaTypeOrExtent]
        end
        # conformsTo
        if conformsTo
          schema = RDF::URI.new(conformsTo)
          g << [identifier, RDF::Vocab::DC.conformsTo, schema]
          g << [schema, RDF.type, RDF::Vocab::DC.Standard]
        end

      end

      # catalog dataset  distribution
      if is_a? DCATCatalog and !datasets.empty?
        datasets.each do |d|
          g << [identifier, DCAT.dataset, RDF::URI.new(d.identifier)]
        end
      elsif is_a? DCATCatalog and !accessServices.empty?
        accessServices.each do |d|
          g << [identifier, DCAT.service, RDF::URI.new(d.identifier)]
        end
      elsif is_a? DCATDataset and !distributions.empty?
        distributions.each do |d|
          g << [identifier, DCAT.distribution, RDF::URI.new(d.identifier)]
        end
      elsif is_a? DCATDistribution and !accessServices.empty?
        accessServices.each do |d|
          g << [identifier, DCAT.accessService, RDF::URI.new(d.identifier)]
        end
      end

      # theme
      return unless theme

      themes = theme.split(",").filter_map { |url| url.strip unless url.strip.empty? }
      themes.each do |theme|
        g << [identifier, DCAT.theme, RDF::URI.new(theme)]
        g << [RDF::URI.new(theme), RDF.type, RDF::Vocab::SKOS.Concept]
        g << [RDF::URI.new(theme), RDF::Vocab::SKOS.inScheme,
              RDF::URI.new(identifier.to_s + "#conceptscheme")]
      end
      g << [RDF::URI.new(identifier.to_s + "#conceptscheme"), RDF.type,
            RDF::Vocab::SKOS.ConceptScheme]
    end

    def serialize(format: :turtle)
      @g.dump(:turtle)
    end

    def publish
      location = identifier.to_s.gsub(baseURI, serverURL)
      begin
        resp = RestClient.put("#{location}/meta/state", '{ "current": "PUBLISHED" }',
                              headers = { authorization: "Bearer #{$token}", content_type: "application/json" })
        warn "publish response message"
        warn resp.inspect
      rescue StandardError
        warn "ERROR in publishing"
      end
    end

    def get_pred_value(pred, vocab, datatype = nil)
      # $stderr.puts "getting #{pred}, #{vocab}"
      urire = Regexp.new("((http|https)://)(www.)?[a-zA-Z0-9@:%._\\+~#?&//=]{2,256}\\.[a-z]{2,8}\\b([-a-zA-Z0-9@:%._\\+~#?&//=]*)")
      sym = "@" + pred
      # $stderr.puts "getting #{pred}, #{sym}..."
      case vocab
      when "DCT"
        pred = RDF::Vocab::DC[pred]
      when "DCAT"
        pred = DCAT[pred]
      when "FOAF"
        pred = FOAF[pred]
      end
      # $stderr.puts "got #{pred}, #{vocab}"

      value = instance_variable_get(sym).to_s
      thisvalue = value # temp compy
      # $stderr.puts "got2 #{pred}, #{value}"

      if datatype == "TIME"
        now = Time.now.strftime("%Y-%m-%dT%H:%M:%S.%L")
        value = RDF::Literal.new(thisvalue, datatype: RDF::URI("http://www.w3.org/2001/XMLSchema#dateTime"))
        warn "time value1 #{value}"
        unless value.valid?
          thisvalue += "T12:00+01:00" # make a guess that they only provided the date
          value = RDF::Literal.new(thisvalue, datatype: RDF::URI("http://www.w3.org/2001/XMLSchema#dateTime"))
          warn "time value2 #{value}"
          unless value.valid?
            value = RDF::Literal.new(now, datatype: RDF::URI("http://www.w3.org/2001/XMLSchema#dateTime"))
            warn "time value3 #{value}"
          end
        end
      elsif urire.match(thisvalue)
        value = RDF::URI.new(thisvalue)
      end
      return [nil, nil] if value.to_s.empty?

      warn "returning #{pred}, #{value}"
      [pred, value]
    end
  end

  # %w() array of strings
  # %r() regular expression.
  # %q() string
  # %x() a shell command (returning the output string)
  # %i() array of symbols (Ruby >= 2.0.0)
  # %s() symbol
  # %() (without letter) shortcut for %Q()
end
