# frozen_string_literal: true

require "rest-client"
require "json"

module FDP
  ##
  # Client for interacting with a FAIR Data Point (FDP) reference implementation server.
  #
  # This class handles authentication (token-based) and provides methods for
  # discovering and managing metadata schemas (SHACL shapes) and resource definitions.
  #
  # == Authentication
  #
  # The client obtains a JWT bearer token via the +/tokens+ endpoint on initialization.
  # All subsequent requests use this token in the +Authorization+ header.
  #
  # == Usage example
  #
  #   client = FDP::Client.new(
  #     base_url: "https://example.com/fdp",
  #     email:    "user@example.com",
  #     password: "secret123"
  #   )
  #
  #   schemas  = client.retrieve_current_schemas
  #   resources = client.retrieve_current_resources
  #
  class Client
    # @return [String] Base URL of the FAIR Data Point server (without trailing slash preferred)
    attr_accessor :base_url

    # @return [String] Email used for authentication
    attr_accessor :email

    # @return [String] Password used for authentication (stored only temporarily)
    attr_accessor :password

    # @return [String] JWT bearer token obtained after successful login
    attr_accessor :token

    # @return [Hash] Default headers used in all authenticated API requests
    attr_accessor :headers

    ##
    # Initializes a new FDP client and authenticates against the server.
    #
    # @param base_url [String] URL of the FAIR Data Point server (e.g. https://fdp.example.com)
    # @param email    [String] User email for authentication (default: albert.einstein@example.com)
    # @param password [String] User password for authentication (default: password)
    #
    # @raise [SystemExit] if authentication fails or unexpected error occurs
    #
    def initialize(base_url:, email: "albert.einstein@example.com", password: "password")
      @base_url = base_url
      @email    = email
      @password = password

      begin
        response = RestClient.post(
          "#{base_url}/tokens",
          { email: email, password: password }.to_json,
          content_type: :json, accept: :json
        )

        token_data = JSON.parse(response.body)
        @token = token_data["token"]

        warn "Authorization: Bearer #{@token}"
      rescue RestClient::ExceptionWithResponse => e
        warn "Error getting token:"
        warn "Status: #{e.response.code}"
        warn "Body: #{e.response.body}"
        abort
      rescue StandardError => e
        warn "Unexpected error: #{e.message}"
        abort
      end

      @headers = {
        Authorization: "Bearer #{@token}",
        accept: :json,
        content_type: :json
      }
    end

    ##
    # Fetches a simple name → {uuid, definition} lookup of currently defined metadata schemas.
    #
    # @return [Hash<String, Hash>] schema name → { 'uuid' => ..., 'definition' => ... }
    # @return [Hash] empty hash when request fails
    #
    def list_current_schemas
      begin
        response = RestClient.get("#{base_url}/metadata-schemas", headers)
      rescue RestClient::ExceptionWithResponse => e
        warn "Error fetching schemas:"
        warn "Status: #{e.response.code}"
        warn "Body: #{e.response.body}"
        return {}
      rescue StandardError => e
        warn "Unexpected error: #{e.message}"
        return {}
      end

      j = JSON.parse(response.body)
      uuids = {}

      j.each do |entry|
        uuids[entry["name"]] = {
          "uuid" => entry["uuid"],
          "definition" => entry["latest"]["definition"]
        }
      end

      uuids
    end

    ##
    # Retrieves all currently defined metadata schemas as rich {FDP::Schema} objects.
    #
    # This method is usually preferred over #list_current_schemas when you need
    # full metadata shape information (parents, prefix, target classes, etc.).
    #
    # @return [Array<FDP::Schema>] array of schema objects
    # @return [Array] empty array when request fails
    #
    def retrieve_current_schemas
      begin
        response = RestClient.get("#{base_url}/metadata-schemas", headers)
      rescue RestClient::ExceptionWithResponse => e
        warn "Error fetching schemas:"
        warn "Status: #{e.response.code}"
        warn "Body: #{e.response.body}"
        return []
      rescue StandardError => e
        warn "Unexpected error: #{e.message}"
        return []
      end

      j = JSON.parse(response.body)
      schemas = []

      j.each do |entry|
        latest = entry["latest"] || {}

        schemas << FDP::Schema.new(
          client: self,
          uuid: entry["uuid"],
          name: entry["name"],
          label: latest["suggestedResourceName"],
          description: latest["description"],
          definition: latest["definition"],
          prefix: latest["suggestedUrlPrefix"],
          parents: latest["extendsSchemaUuids"] || [],
          children: latest["childSchemaUuids"] || [],
          version: latest["version"] || "1.0.0",
          targetclasses: latest["targetClassUris"] || ["http://www.w3.org/ns/dcat#Resource"]
        )
      end

      schemas
    end

    ##
    # Fetches a simple name → uuid lookup of currently defined resource definitions.
    #
    # @return [Hash<String, Hash>] resource name → { 'uuid' => ... }
    # @return [Hash] empty hash when request fails
    #
    def list_current_resources
      begin
        response = RestClient.get("#{base_url}/resource-definitions", headers)
      rescue RestClient::ExceptionWithResponse => e
        warn "Error fetching resources definitions:"
        warn "Status: #{e.response.code}"
        warn "Body: #{e.response.body}"
        return {}
      rescue StandardError => e
        warn "Unexpected error: #{e.message}"
        return {}
      end

      j = JSON.parse(response.body)
      uuids = {}

      j.each do |entry|
        uuids[entry["name"]] = { "uuid" => entry["uuid"] }
      end

      uuids
    end

    ##
    # Retrieves all currently defined resource definitions as rich {FDP::Resource} objects.
    #
    # @return [Array<FDP::Resource>] array of resource definition objects
    # @return [Array] empty array when request fails
    #
    def retrieve_current_resources
      begin
        response = RestClient.get("#{base_url}/resource-definitions", headers)
      rescue RestClient::ExceptionWithResponse => e
        warn "Error fetching resources definitions:"
        warn "Status: #{e.response.code}"
        warn "Body: #{e.response.body}"
        return []
      rescue StandardError => e
        warn "Unexpected error: #{e.message}"
        return []
      end

      j = JSON.parse(response.body)
      resources = []

      j.each do |entry|
        resources << FDP::Resource.new(resourcejson: entry, client: self)
      end

      resources
    end
  end
end
