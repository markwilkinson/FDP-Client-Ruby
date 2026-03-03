# frozen_string_literal: true

require "rest-client" # assumed already required upstream
require "json"

module FDP
  ##
  # Represents a resource definition in a FAIR Data Point server.
  #
  # Resource definitions describe types of metadata records that can be registered
  # in the FDP (e.g. Dataset, Data Service, Repository, or custom types like Biobank).
  # Each resource definition specifies:
  #
  # - which metadata schemas (SHACL shapes) apply,
  # - target RDF classes (usually dcat:Resource or subclasses),
  # - hierarchical children (nested resource types),
  # - external links/properties shown in the UI.
  #
  # Resource names must be unique. New resources are created via POST to
  # +/resource-definitions+, existing ones updated via PUT to
  # +/resource-definitions/{uuid}+.
  #
  # @see FDP::Client#retrieve_current_resources
  # @see FDP::Client#list_current_resources
  # @see FDP::Schema   for the metadata shapes applied to these resources
  #
  class Resource
    # @return [FDP::Client] Client used for API operations
    attr_accessor :client

    # @return [String, nil] UUID of the resource definition (nil if new)
    attr_accessor :uuid

    # @return [String] Unique internal name (no spaces/special chars recommended)
    attr_accessor :name

    # @return [String, nil] URL prefix suggestion for generated resource URLs
    attr_accessor :prefix

    # @return [Array<String>] UUIDs of metadata schemas (SHACL shapes) that apply
    attr_accessor :schemas

    # @return [Array<String>] Target RDF class URIs this resource represents
    attr_accessor :targeturis

    # @return [Array<FDP::ResourceChild>] Nested/child resource definitions
    attr_accessor :children

    # @return [Array<FDP::ResourceExternalLink>] External links shown in UI
    attr_accessor :external_links

    # @return [String] Human-readable description
    attr_accessor :description

    ##
    # Initializes a new Resource definition.
    #
    # @overload initialize(client:, resourcejson:)
    #   Create from existing API response (parsed JSON)
    #   @param resourcejson [Hash] Parsed JSON from /resource-definitions endpoint
    #
    # @overload initialize(client:, name:, schemas:, ...)
    #   Create a new resource definition (to be written to server)
    #   @param name          [String] Unique name (checked if uuid nil)
    #   @param schemas       [Array<String>] Schema UUIDs
    #   @param description   [String, nil]
    #   @param prefix        [String, nil]
    #   @param targeturis    [Array<String>]
    #   @param children      [Array<FDP::ResourceChild>]
    #   @param external_links [Array<FDP::ResourceExternalLink>]
    #   @param uuid          [String, nil] Existing UUID
    #
    # @raise [ArgumentError] if name already exists on server (when creating new)
    #
    def initialize(client:, resourcejson: nil, schemas: [], description: nil, prefix: nil,
                   targeturis: [], children: [], external_links: [], uuid: nil, name: nil)
      @client         = client
      @uuid           = uuid
      @name           = name
      @description    = description
      @prefix         = prefix
      @targeturis     = targeturis
      @children       = children
      @external_links = external_links

      if resourcejson
        # Hydrate from existing server response
        @uuid           = resourcejson["uuid"]
        @name           = resourcejson["name"]
        @prefix         = resourcejson["urlPrefix"]
        @schemas        = resourcejson["metadataSchemaUuids"] || []
        @targeturis     = resourcejson["targetClassUris"]     || []
        @description    = resourcejson["description"] || "No description provided"

        @children = (resourcejson["children"] || []).map do |childjson|
          ResourceChild.new(childjson: childjson)
        end

        @external_links = (resourcejson["externalLinks"] || []).map do |linkjson|
          ResourceExternalLink.new(linkjson: linkjson)
        end
      else
        # New resource — enforce name uniqueness
        @schemas        = schemas
        @description    = description || "No description provided"
        validate_name(name: name) unless uuid
      end
    end

    ##
    # Validates that the resource name does not already exist on the server.
    #
    # @param name [String]
    #
    # @raise [ArgumentError] if name is taken
    #
    def validate_name(name:)
      return unless @client.list_current_resources.key?(name)

      raise ArgumentError,
            "Resource name '#{name}' already exists on the server. " \
            "Update the existing resource rather than creating a duplicate."
    end

    ##
    # Builds the payload suitable for POST/PUT to /resource-definitions.
    #
    # Removes duplicate children/links based on their #key.
    #
    # @return [Hash] API-ready payload with string keys
    #
    def to_api_payload
      # warn "CHILDreN vbefore #{children.inspect}"
      # warn "Links vbefore #{external_links.inspect}"
      if children&.first
        grouped = children.group_by { |c| c.key } # ← explicit block, proven to work
        children = grouped.values.map(&:first)
      end
      # same for external_links
      if external_links&.first
        grouped = external_links.group_by { |c| c.key }
        external_links = grouped.values.map(&:first)
      end
      children = [] if children.nil?
      external_links = [] if external_links.nil?
      # warn "CHILDreN after #{children.inspect}"
      # warn "Links after #{external_links.inspect}"

      payload = {
        uuid: uuid,
        name: name,
        urlPrefix: prefix,
        metadataSchemaUuids: schemas,
        targetClassUris: targeturis,
        children: children.map(&:to_api_payload),
        externalLinks: external_links.map(&:to_api_payload),
        description: description
      }

      payload.transform_keys(&:to_s)
    end

    ##
    # Writes this resource definition to the FDP server (create or update).
    #
    # @param client [FDP::Client] (defaults to @client)
    #
    # @return [self, nil] self on success, nil on failure
    #
    def write_to_fdp(client: @client)
      if uuid.to_s.strip.empty?
        write_new_resource(client: client)
      else
        replace_existing_resource(client: client)
      end
    end

    private

    ##
    # Creates a new resource definition via POST.
    #
    # @param client [FDP::Client]
    #
    # @return [self, nil]
    #
    def write_new_resource(client:)
      payload = to_api_payload
      payload.delete("uuid") # server generates UUID

      warn "Creating new resource definition '#{name}'"

      begin
        response = RestClient.post(
          "#{client.base_url}/resource-definitions",
          payload.to_json,
          client.headers
        )

        result = JSON.parse(response.body)
        # warn "Resource '#{name}' created successfully with UUID: #{result["uuid"]}"
        self.uuid = result["uuid"]
        self
      rescue RestClient::ExceptionWithResponse => e
        warn "Create failed for resource '#{name}' (HTTP #{e.response.code}):"
        warn e.response.body
        nil
      rescue StandardError => e
        warn "Unexpected error creating resource '#{name}': #{e.message}"
        nil
      end
    end

    ##
    # Replaces (updates) an existing resource definition via PUT.
    #
    # @param client [FDP::Client]
    #
    # @return [self, nil]
    #
    def replace_existing_resource(client:)
      payload = to_api_payload

      warn "Replacing resource definition '#{name}' (UUID: #{uuid})"

      begin
        RestClient.put(
          "#{client.base_url}/resource-definitions/#{uuid}",
          payload.to_json,
          client.headers
        )

        # warn "Resource '#{name}' updated successfully."
        self
      rescue RestClient::ExceptionWithResponse => e
        warn "Update failed for resource '#{name}' (HTTP #{e.response.code}):"
        warn e.response.body
        nil
      rescue StandardError => e
        warn "Unexpected error updating resource '#{name}': #{e.message}"
        nil
      end
    end
  end

  ##
  # Represents a child resource reference within a parent {FDP::Resource}.
  #
  # Defines a nested/hierarchical relationship (e.g. a Dataset inside a Repository).
  #
  class ResourceChild
    # @return [String] UUID of the child resource definition
    attr_accessor :resourceDefinitionUuid

    # @return [String] RDF property URI expressing the parent → child relation
    attr_accessor :relationUri

    # @return [String] Title shown in list views
    attr_accessor :listViewTitle

    # @return [String, nil] URI for tags displayed in list views
    attr_accessor :listViewTagsUri

    # @return [Array] Metadata fields shown in list views
    attr_accessor :listViewMetadata

    ##
    # @overload initialize(childjson:)
    #   From existing API response
    # @overload initialize(resourceDefinitionUuid:, relationUri:, ...)
    #   New child definition
    #
    # @raise [ArgumentError] if required fields missing in new mode
    #
    def initialize(childjson: nil, resourceDefinitionUuid: nil, relationUri: nil,
                   listViewTitle: nil, listViewTagsUri: nil, listViewMetadata: [])
      if childjson
        @resourceDefinitionUuid = childjson["resourceDefinitionUuid"]
        @relationUri            = childjson["relationUri"]
        list_view               = childjson["listView"] || {}
        @listViewTitle          = list_view["title"]    || "No title provided"
        @listViewTagsUri        = list_view["tagsUri"]
        @listViewMetadata       = list_view["metadata"] || []
      else
        raise ArgumentError, "resourceDefinitionUuid and relationUri are required" \
          unless resourceDefinitionUuid && relationUri

        @resourceDefinitionUuid = resourceDefinitionUuid
        @relationUri            = relationUri
        @listViewTitle          = listViewTitle
        @listViewTagsUri        = listViewTagsUri
        @listViewMetadata       = listViewMetadata
      end
    end

    ##
    # Unique key used for de-duplication within a parent resource.
    #
    # @return [Array<String>]
    #
    def key
      [resourceDefinitionUuid, relationUri]
    end

    ##
    # @return [Hash] API-ready payload
    #
    def to_api_payload
      {
        resourceDefinitionUuid: resourceDefinitionUuid,
        relationUri: relationUri,
        listView: {
          title: listViewTitle,
          tagsUri: listViewTagsUri,
          metadata: listViewMetadata
        }
      }.transform_keys(&:to_s)
    end
  end

  ##
  # Represents an external link / property displayed for a resource in the FDP UI.
  #
  class ResourceExternalLink
    # @return [String] Display title of the link
    attr_accessor :title

    # @return [String] RDF property URI this link represents
    attr_accessor :propertyuri

    ##
    # @overload initialize(linkjson:)
    #   From API response
    # @overload initialize(title:, propertyuri:)
    #   New external link
    #
    def initialize(linkjson: nil, title: nil, propertyuri: nil)
      if linkjson
        @title       = linkjson["title"]
        @propertyuri = linkjson["propertyUri"]
      else
        @title       = title
        @propertyuri = propertyuri
      end
    end

    ##
    # Unique key for de-duplication.
    #
    # @return [Array<String>]
    #
    def key
      [propertyuri, title]
    end

    ##
    # @return [Hash] API-ready payload
    #
    def to_api_payload
      {
        title: title,
        propertyUri: propertyuri
      }.transform_keys(&:to_s)
    end
  end
end
