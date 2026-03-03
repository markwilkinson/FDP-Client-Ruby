module FDPMate
  class DCATCatalog < DCATResource
    attr_accessor :primaryTopic, :datasets, :themeTaxonomy, :accessServices

    # def initialize(primary_topic: nil, baseuri: "http://example.org", access_rights: nil, conforms_to: nil, contact_point: nil, resource_creator: nil,
    #     title: nil, release_date: nil, modification_date: nil, publisher: nil, identifier: nil, license: nil  )
    def initialize(themeTaxonomy: nil, **args)
      super
      # warn "initialize"
      @datasets = []
      @accessServices = []
      @themeTaxonomy = themeTaxonomy
      self.types = [DCAT.Catalog, DCAT.Resource]
      init_catalog # create record and get GUID
      build # make the RDF
      write_catalog
    end

    def init_catalog
      warn "initializing catalog"
      catinit = <<~END
        @prefix dcat: <http://www.w3.org/ns/dcat#> .
        @prefix dct: <http://purl.org/dc/terms/> .
        @prefix foaf: <http://xmlns.com/foaf/0.1/> .

        <> a dcat:Catalog, dcat:Resource ;
            dct:title "test" ;
            dct:hasVersion "1.0" ;
            dct:publisher [ a foaf:Agent ; foaf:name "Example User" ] ;
            dct:isPartOf <#{@parentURI}> .
END

      warn "#{serverURL}/catalog\n\n"
      # warn "#{catinit}\n\n"
      # $stderr.puts catinit
      resp = RestClient.post("#{serverURL}/catalog", catinit, $headers)
      catlocation = resp.headers[:location]
      warn "temporary catalog written to #{catlocation}\n\n"
      self.identifier = RDF::URI(catlocation) # set identifier to where it lives
    end

    def write_catalog
      build
      location = identifier.to_s.gsub(baseURI, serverURL)
      warn "rewriting cat to #{location}"
      catalog = serialize
      # warn catalog
      resp = RestClient.put(location, catalog, $headers)
      warn resp.headers
    end

    def add_dataset(dataset:)
      @datasets << dataset
    end
  end
end
