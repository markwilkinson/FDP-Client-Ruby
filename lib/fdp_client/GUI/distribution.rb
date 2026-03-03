module FDPMate
  class DCATDistribution < DCATResource
    attr_accessor :mediaType, :format, :accessServices

    def initialize(parent_dataset:, mediaType: nil, format: nil, **args)
      @accessServices = []
      @mediaType = mediaType
      @format = format

      super

      self.types = [DCAT.Resource, DCAT.Distribution]
      init_distribution # create record and get GUID
      build # make the RDF
      write_distribution
      parent_dataset.distributions << self
    end

    def init_distribution
      warn "initializing distribution"
      distinit = <<~END
        @prefix dcat: <http://www.w3.org/ns/dcat#> .
        @prefix dct: <http://purl.org/dc/terms/> .
        @prefix foaf: <http://xmlns.com/foaf/0.1/> .
        <> a dcat:Distribution, dcat:Resource ;
            dct:title "test" ;
            dct:hasVersion "1.0" ;
            dct:publisher [ a foaf:Agent ; foaf:name "Example User" ] ;
            dct:isPartOf <#{@parentURI}> ;
            dcat:mediaType "text/plain" .
END

      warn "#{serverURL}/distribution"
      warn "#{distinit}\n\n"
      resp = RestClient.post("#{@serverURL}/distribution", distinit, $headers)
      distlocation = resp.headers[:location]
      puts "temporary distribution written to #{distlocation}\n\n"
      self.identifier = RDF::URI(distlocation) # set identifier to where it lives
    end

    def write_distribution
      build
      location = identifier.to_s.gsub(@baseURI, @serverURL)
      warn "rewriting distribution to #{location}"
      distribution = serialize
      warn distribution
      resp = RestClient.put(location, distribution, $headers)
      warn resp.headers
    end

    def add_accessService(accessService:)
      @accessServices << accessService
    end
  end
end
