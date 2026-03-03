# frozen_string_literal: true

require "rest-client" # assuming already required in client or elsewhere
require "json"

module FDP
  ##
  # Represents a metadata schema (SHACL shape) in a FAIR Data Point server.
  #
  # This class models a custom or extended metadata shape used for validating
  # resource metadata records in an FDP instance. Schemas can extend others
  # (inheritance via parents), target specific classes (usually subclasses of
  # dcat:Resource), and contain the raw Turtle SHACL definition.
  #
  # == Key concepts
  #
  # - Schemas are identified by a unique +name+ (internal identifier).
  # - The FDP enforces uniqueness on schema names — you cannot create a new
  #   schema with an existing name; you must update the existing one.
  # - Schemas support versioning; updates typically increment the patch version.
  # - New schemas are created via POST to +/metadata-schemas+.
  # - Existing schemas are updated via PUT to +/metadata-schemas/{uuid}/draft+,
  #   followed by publishing via POST to +/metadata-schemas/{uuid}/versions+.
  #
  # @see FDP::Client for fetching and writing schemas
  #
  class Schema
    # @return [FDP::Client] The client used to interact with the FDP server
    attr_accessor :client

    # @return [String, nil] UUID of the schema (nil for new/unpersisted schemas)
    attr_accessor :uuid

    # @return [String] Unique internal name (no spaces/special chars recommended)
    attr_accessor :name

    # @return [String] Human-readable label (suggested resource name in UI)
    attr_accessor :label

    # @return [String] Description of the schema
    attr_accessor :description

    # @return [String, nil] Suggested URL prefix for resources using this shape
    attr_accessor :prefix

    # @return [String] Raw Turtle SHACL definition string
    attr_accessor :definition

    # @return [String] Version string (SemVer-like, e.g. "1.0.0")
    attr_accessor :version

    # @return [Array<String>] UUIDs of parent schemas this one extends
    attr_accessor :parents

    # @return [Array<String>] UUIDs of child schemas that extend this one
    attr_accessor :children

    # @return [Array<String>] Target class URIs (must include dcat:Resource)
    attr_accessor :targetclasses

    # @return [Boolean] Whether this is an abstract schema (not directly assignable)
    attr_accessor :abstractschema

    ##
    # Initializes a new Schema object.
    #
    # @param client       [FDP::Client] Required client for API operations
    # @param name         [String] Unique schema name (checked against server if uuid nil)
    # @param label        [String] Display name
    # @param description  [String] Schema description
    # @param definition   [String] Turtle SHACL content
    # @param prefix       [String, nil] Optional URL prefix suggestion
    # @param version      [String] Version (default "1.0.0" suggested)
    # @param targetclasses [Array<String>] Target class URIs
    # @param parents      [Array<String>] Parent schema UUIDs (default [])
    # @param children     [Array<String>] Child schema UUIDs (default [])
    # @param uuid         [String, nil] Existing UUID (nil for new schema)
    # @param abstractschema [Boolean] Abstract flag (default false)
    #
    # @raise [ArgumentError] if name already exists on server (when creating new)
    #
    def initialize(client:, name:, label:, description:, definition:, prefix:, version:,
                   targetclasses:, parents: [], children: [], uuid: nil, abstractschema: false)
      @client       = client
      @uuid         = uuid
      @name         = name
      @label        = label
      @description  = description
      @prefix       = prefix
      @definition   = definition
      @version      = version
      @parents      = parents
      @children     = children
      @targetclasses = ["http://www.w3.org/ns/dcat#Resource", *targetclasses].uniq
      @abstractschema = abstractschema

      # Enforce name uniqueness when creating a new schema
      validate_name(name: name) unless uuid
    end

    ##
    # Validates that the schema name does not already exist on the server.
    #
    # @param name [String] Name to check
    #
    # @raise [ArgumentError] if name is already taken
    #
    def validate_name(name:)
      return unless @client.list_current_schemas.key?(name)

      raise ArgumentError,
            "Schema name '#{name}' already exists on the server. " \
            "You must update the existing schema rather than create a duplicate."
    end

    ##
    # Builds the payload suitable for POST/PUT to the FDP /metadata-schemas endpoint.
    #
    # @return [Hash] API-ready payload with string keys
    #
    def to_api_payload
      @children.uniq!
      @parents.uniq!

      payload = {
        uuid: uuid, # nil for create → server generates
        name: name,
        description: description,
        abstractSchema: abstractschema,
        suggestedResourceName: label,
        suggestedUrlPrefix: prefix,
        published: true, # we publish immediately after
        definition: definition,
        extendsSchemaUuids: parents,
        version: version,
        targetClasses: targetclasses,
        childSchemaUuids: children
      }

      # Ensure string keys (some API versions may be strict)
      payload.transform_keys(&:to_s)
    end

    ##
    # Writes this schema to the FDP server (create if new, overwrite if existing).
    #
    # Automatically handles create vs. update, version increment, and publishing.
    #
    # @param client [FDP::Client] Client to use (usually same as +@client+)
    #
    # @return [String, nil] Response body on success, nil on failure
    #
    def write_to_fdp(client: @client)
      # warn "Current UUID for schema '#{name}': #{uuid || "(new)"}"

      if uuid.to_s.strip.empty?
        write_new_schema_to_fdp(client: client)
      else
        warn "Schema '#{name}' has UUID #{uuid}. Overwriting with new definition."
        warn "Change name or clear uuid if you want to create a new version instead."
        overwrite_schema_in_fdp(client: client)
      end
    end

    ##
    # Increments the patch version (adds 1, or custom logic).
    #
    # Currently adds +5+ to patch — consider changing to +1+ for standard SemVer.
    #
    # @return [String] New version string
    #
    def increment_version
      # warn "Incrementing patch version for schema '#{name}' (current: #{version})"
      major, minor, patch = version.split(".").map(&:to_i)
      patch += 1
      new_version = "#{major}.#{minor}.#{patch}"
      warn "New version: #{new_version}"
      new_version
    end

    private

    ##
    # Overwrites an existing schema (PUT to draft, then publish).
    #
    # @param client [FDP::Client]
    #
    # @return [String, nil]
    #
    def overwrite_schema_in_fdp(client:)
      payload = to_api_payload
      new_version = increment_version
      payload["version"] = new_version
      self.version = new_version # local update (API may ignore client-supplied version)

      warn "\nOVERWRITING schema '#{name}' (UUID: #{uuid}) with:\n#{JSON.pretty_generate(payload)}\n"

      begin
        response = RestClient.put(
          "#{client.base_url}/metadata-schemas/#{uuid}/draft",
          payload.to_json,
          client.headers
        )

        result = JSON.parse(response.body)
        # warn "Schema '#{name}' updated as draft (version #{version})"
        self.uuid = result["uuid"] || uuid # usually unchanged

        publish(client: client)
      rescue RestClient::ExceptionWithResponse => e
        warn "Overwrite failed for '#{name}' (HTTP #{e.response.code}):"
        warn e.response.body
        nil
      rescue StandardError => e
        warn "Unexpected error overwriting '#{name}': #{e.message}"
        nil
      end
    end

    ##
    # Creates a new schema on the server, then publishes it.
    #
    # @param client [FDP::Client]
    #
    # @return [String, nil]
    #
    def write_new_schema_to_fdp(client:)
      payload = to_api_payload
      payload.delete("uuid") # server generates UUID

      warn "\nCREATING new schema '#{name}' with:\n#{JSON.pretty_generate(payload)}\n"

      begin
        response = RestClient.post(
          "#{client.base_url}/metadata-schemas",
          payload.to_json,
          client.headers
        )

        result = JSON.parse(response.body)
        # warn "Schema '#{name}' created with UUID: #{result["uuid"]}"
        self.uuid = result["uuid"]

        publish(client: client)
      rescue RestClient::ExceptionWithResponse => e
        warn "Create failed for '#{name}' (HTTP #{e.response.code}):"
        warn e.response.body
        nil
      rescue StandardError => e
        warn "Unexpected error creating '#{name}': #{e.message}"
        nil
      end
    end

    ##
    # Publishes the current schema version (moves out of draft).
    #
    # Note: The FDP API requires +published: false+ in this payload to actually publish.
    # This is counter-intuitive but matches observed reference implementation behavior.
    #
    # @param client [FDP::Client]
    #
    # @return [String, nil] Response body or nil
    #
    def publish(client:)
      publish_payload = {
        description: description,
        version: version,
        published: false # ← critical: API uses false here to trigger publish
      }.to_json

      # warn "\nPublishing schema '#{name}' (UUID: #{uuid}) with:\n#{JSON.pretty_generate(JSON.parse(publish_payload))}\n"

      begin
        response = RestClient.post(
          "#{client.base_url}/metadata-schemas/#{uuid}/versions",
          publish_payload,
          client.headers
        )

        # warn "Schema '#{name}' published successfully."
        response.body
      rescue RestClient::ExceptionWithResponse => e
        warn "Publish failed for '#{name}' (HTTP #{e.response.code}):"
        warn e.response.body
        nil
      rescue StandardError => e
        warn "Unexpected error publishing '#{name}': #{e.message}"
        nil
      end
    end
  end
end
