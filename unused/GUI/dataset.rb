module FDPMate
  class DCATDataset < DCATResource
    attr_accessor :was_generated_by, :distribution, :theme, :landingPage, :distributions

    def initialize(parent_catalog:, theme: nil, landingPage: nil, **args)
      super
      @theme = theme
      @landingPage = landingPage
      @distributions = []

      self.types = [DCAT.Resource, DCAT.Dataset]

      init_dataset   # create record and get GUID
      build # make the RDF
      write_dataset
      parent_catalog.datasets << self
    end

    def init_dataset
      warn "initializing dataset"
      dsetinit = <<~END
        @prefix dcat: <http://www.w3.org/ns/dcat#> .
        @prefix dct: <http://purl.org/dc/terms/> .
        @prefix foaf: <http://xmlns.com/foaf/0.1/> .
        <> a dcat:Dataset, dcat:Resource ;
            dct:title "test" ;
            dct:hasVersion "1.0" ;
            dct:publisher [ a foaf:Agent ; foaf:name "Example User" ] ;
            dcat:theme <http://exampletheme.org/> ;
            dct:isPartOf <#{@parentURI}> .
END

      warn "#{@serverURL}/dataset"
      warn "#{dsetinit}\n\n"
      warn dsetinit
      resp = RestClient.post("#{@serverURL}/dataset", dsetinit, $headers)
      dsetlocation = resp.headers[:location]
      puts "temporary dataset written to #{dsetlocation}\n\n"
      self.identifier = RDF::URI(dsetlocation) # set identifier to where it lives
    end

    def write_dataset
      build
      location = identifier.to_s.gsub(baseURI, serverURL)
      warn "rewriting dset to #{location}"
      dataset = serialize
      warn dataset
      resp = RestClient.put(location, dataset, $headers)
      warn resp.headers
    end

    # def datasets
    #   return $datasets
    # end
    # def self.datasets
    #   return $datasets
    # end
  end
end
