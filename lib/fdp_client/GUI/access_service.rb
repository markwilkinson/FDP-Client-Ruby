module FDPMate
  class DCATDataService < DCATResource
    attr_accessor :endpointDescription, :endpointURL

    def initialize(parent:, endpointDescription: nil, endpointURL: nil, **args)
      super(**args)
      @endpointDescription = endpointDescription
      @endpointURL = endpointURL

      warn "Building Data Service"
      warn self.endpointDescription, self.endpointURL, self.class

      self.types = [DCAT.Resource, DCAT.DataService]
      init_accessService # create record and get GUID
      build # make the RDF
      write_accessService
      parent.accessServices << self
    end

    def init_accessService
      warn "initializing access Service"
      asinit = <<~END
        @prefix dcat: <http://www.w3.org/ns/dcat#> .
        @prefix dct: <http://purl.org/dc/terms/> .
        @prefix foaf: <http://xmlns.com/foaf/0.1/> .
        <> a dcat:DataService ;
            foaf:name "test" ;
            dct:title "test" ;
            dct:hasVersion "1.0" ;
            dcat:endpointURL <https://example.org> ;
            dct:publisher [ a foaf:Agent ; foaf:name "Example User" ] ;
            dct:isPartOf <#{@parentURI}> .
END

      warn "#{serverURL}/dataService"
      # warn "#{asinit}\n\n"
      resp = RestClient.post("#{serverURL}/dataService", asinit, $headers)
      aslocation = resp.headers[:location]
      puts "temporary distribution written to #{aslocation}\n\n"
      self.identifier = RDF::URI(aslocation) # set identifier to where it lives
    end

    def write_accessService
      build
      location = identifier.to_s.gsub(baseURI, serverURL)
      warn "rewriting access service to #{location}"
      ds = serialize
      # warn ds
      resp = RestClient.put(location, ds, $headers)
      warn resp.headers
    end
  end
end
