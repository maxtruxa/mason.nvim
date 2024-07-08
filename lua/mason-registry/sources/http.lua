local Optional = require "mason-core.optional"
local Result = require "mason-core.result"
local _ = require "mason-core.functional"
local fetch = require "mason-core.fetch"
local fs = require "mason-core.fs"
local log = require "mason-core.log"
local path = require "mason-core.path"
local util = require "mason-registry.sources.util"

---@class HttpRegistrySourceSpec
---@field id string
---@field name string
---@field url string

---@class HttpRegistrySource : RegistrySource
---@field id string
---@field spec HttpRegistrySourceSpec
---@field root_dir string
---@field private data_file string
---@field private info_file string
---@field buffer table<string, Package>?
local HttpRegistrySource = {}
HttpRegistrySource.__index = HttpRegistrySource

---@param spec HttpRegistrySourceSpec
function HttpRegistrySource.new(spec)
    local root_dir = path.concat { path.registry_prefix(), "http", spec.name }
    return setmetatable({
        id = spec.id,
        spec = spec,
        root_dir = root_dir,
        data_file = path.concat { root_dir, "registry.json" },
        info_file = path.concat { root_dir, "info.json" },
    }, HttpRegistrySource)
end

function HttpRegistrySource:is_installed()
    return fs.sync.file_exists(self.data_file) and fs.sync.file_exists(self.info_file)
end

---@return RegistryPackageSpec[]
function HttpRegistrySource:get_all_package_specs()
    if not self:is_installed() then
        return {}
    end
    local data = vim.json.decode(fs.sync.read_file(self.data_file)) --[[@as RegistryPackageSpec[] ]]
    return _.filter_map(util.map_registry_spec, data)
end

function HttpRegistrySource:reload()
    if not self:is_installed() then
        return
    end
    self.buffer = _.compose(_.index_by(_.prop "name"), _.map(util.hydrate_package(self.buffer or {})))(
        self:get_all_package_specs()
    )
    return self.buffer
end

function HttpRegistrySource:get_buffer()
    return self.buffer or self:reload() or {}
end

---@param pkg string
---@return Package?
function HttpRegistrySource:get_package(pkg)
    return self:get_buffer()[pkg]
end

function HttpRegistrySource:get_all_package_names()
    return _.map(_.prop "name", self:get_all_package_specs())
end

function HttpRegistrySource:get_installer()
    return Optional.of(_.partial(self.install, self))
end

---@async
function HttpRegistrySource:install()
    local zzlib = require "mason-vendor.zzlib"

    return Result.try(function(try)
        if not fs.async.dir_exists(self.root_dir) then
            log.debug("Creating registry directory", self)
            try(Result.pcall(fs.async.mkdirp, self.root_dir))
        end

        log.trace("Downloading latest registry metadata", self)
        ---@type { checksums: table<string, string>, version: string }
        local info = try(
            fetch(("%s/%s"):format(self.spec.url, "info.json"))
                :map_catching(vim.json.decode)
                :map_err(_.always "Failed to download registry metadata.")
        )
        log.trace("Resolved latest registry version", self, info.version)

        if self:is_installed() and self:get_info().version == info.version then
            -- Version is already installed - nothing to update
            return
        end

        local zip_file = path.concat { self.root_dir, "registry.json.zip" }
        try(fetch(("%s/%s"):format(self.spec.url, "registry.json.zip"), {
            out_file = zip_file,
        }):map_err(_.always "Failed to download registry archive."))
        local zip_buffer = fs.async.read_file(zip_file)
        local registry_contents = try(
            Result.pcall(zzlib.unzip, zip_buffer, "registry.json")
                :on_failure(_.partial(log.error, "Failed to unpack registry archive."))
                :map_err(_.always "Failed to unpack registry archive.")
        )
        pcall(fs.async.unlink, zip_file)

        try(Result.pcall(fs.async.write_file, self.data_file, registry_contents))
        try(Result.pcall(
            fs.async.write_file,
            self.info_file,
            vim.json.encode {
                checksums = info.checksums,
                version = info.version,
                download_timestamp = os.time(),
            }
        ))
    end)
        :on_success(function()
            self:reload()
        end)
        :on_failure(function(err)
            log.fmt_error("Failed to install registry %s. %s", self, err)
        end)
end

---@return { checksums: table<string, string>, version: string, download_timestamp: integer }
function HttpRegistrySource:get_info()
    return vim.json.decode(fs.sync.read_file(self.info_file))
end

function HttpRegistrySource:get_display_name()
    if self:is_installed() then
        local info = self:get_info()
        return ("%s version: %s"):format(self.spec.name, info.version)
    else
        return ("%s [uninstalled]"):format(self.spec.name)
    end
end

function HttpRegistrySource:__tostring()
    return ("HttpRegistrySource(url=%s)"):format(self.spec.url)
end

return HttpRegistrySource
