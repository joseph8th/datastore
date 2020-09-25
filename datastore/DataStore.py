from diskcache import FanoutCache


class DataStore(object):
    """
    Parent class for pure Python diskcache.Fanout sharded key-value storage.
    Can be used alone (no inheritence) but will have no `name` prefix.
    Override `name` and `delim` on inheritence by a child class.

    Instantiates on a single `cache_path` matching any `diskcache.FanoutCache.directory`.
    Ie, inits from a PATH, not a FanoutCache OBJECT. FanoutCache may thus be resized.
    Resize sharding via `self.init_cache()` method. Will COPY data in-place.

    NOTE: Deprecates ConfigurationManager, ExpiringDictionary, CacheManager and actions.json.
    """

    name = ""      # Will become the first element in the key prefix
    delim = ":"    # Separates the elements of the key

    def __init__(self, cache_path, shards=8, timeout=0.01, expire=None, tag_index=False, init_cache=True):
        """
        Init a FanoutCache datastore. Default params are FanoutCache defaults.

        :param str cache_path: Path to the cache dir. I.e., 'cache/data'
        :param int shards: Number of shards to use. Default: 8
        :param float timeout: Seconds until we give up on operation. Default: 0.01
        :param float expire: Seconds after `set` that any given key will expire. Default: None
        :param bool tag_index: Whether to instantiate with tag indexing enabled. Default: False
        :param bool init_cache: Initialize cache now or init with attributes only? Default: True
        """

        self.cache_path = cache_path
        self.shards = int(shards)
        self.timeout = float(timeout)
        self.expire = expire
        self.tag_index = tag_index

        self.cache = None
        self.init_cache()

    def init_cache(self, shards:int=0, timeout:int=0, cache:FanoutCache=None):
        """
        Init or reset (NOT clear!) the underlying DiskCache based on parameters and object attributes.
        If old cache already exists and shards have changed, this COPIES values into new cache to duplicate.
        Does NOT change the cache_path! That means old cache will be REPLACED by new one on disk.
        If you supply a new `diskcache.FanoutCache` object it MUST point to `self.cache_path`!
        Otherwise leave `cache` unset and this method will create a new `FanoutCache` for you.

        :param int shards: If `0` then use `self.shards`
        :param int timeout: If `0` then use `self.timeout`
        :param cache: If `None` then create new `FanoutCache`
        """

        shards = shards or self.shards
        timeout = timeout or self.timeout

        # Allows the calling function to supply a FanoutCache to REPLACE the one on disk
        if cache and cache.directory != self.cache_path:
            raise Exception("Given DiskCache and DataStore paths do not match!")
        cache = cache or FanoutCache(self.cache_path, shards=shards, timeout=timeout, tag_index=self.tag_index)

        # Already have a cache and number of shards changed? We'll have to copy.
        if self.cache and shards != self.shards:
            DataStore.copy_cache(self.cache, cache)

        self.cache = cache
        self.close()

    @staticmethod
    def copy_cache(src_cache, dst_cache):
        """Copy a cache including expiration and tag data. Clears the destination cache first!"""

        dst_cache.clear()
        for key in list(src_cache):
            value, expire_time, tag = src_cache.get(key, expire_time=True, tag=True)
            dst_cache.set(key, value, expire=expire_time, tag=tag)

    def makey(self, *args, as_tuple=False):
        """
        Make-key: return a delim-separated key from a list of strings.
        Starts with self.name if it is set. Not set in base class DataStore.

        :param list args: List of string elements to comprise arbitrary, delimited key.
        :param bool as_tuple: Return a tuple instead of a string? Default: False
        """

        # Make sure we don't repeat an elt by checking for end-delimited self.name like "poop:"
        elts = [] if not self.name or args[0].startswith(self.name+self.delim) else [self.name,]
        elts.extend(args)
        if as_tuple:
            return elts
        return self.delim.join(elts)

    def evict(self, tag):
        """Closing wrapper for DiskCache evict() method."""

        rm_count = self.cache.evict(tag)
        self.close()
        return rm_count

    def expire(self):
        """Closing wrapper for DiskCache expire() method."""

        rm_count = self.cache.expire()
        self.close()
        return rm_count

    def check(self, fix=True):
        """Closing wrapper for DiskCache check() method."""

        warnings = self.cache.check(fix=fix)
        self.close()
        return warnings

    def volume(self):
        """Closing wrapper for DiskCache volume() method."""

        volume = self.cache.volume()
        self.close()
        return volume

    def close(self):
        """Closing wrapper for DiskCache close() method."""

        self.cache.close()

    ############################################################################
    ## Getters
    ############################################################################

    def get(self, key, default=None, read=False, expire_time=False, tag=False, retry=True, close=True):
        """Close-able wrapper for Diskcache get() method."""

        if isinstance(key, (tuple, list)):
            key = self.makey(*key)

        value = self.cache.get(key, default=default, read=read, expire_time=expire_time, tag=tag, retry=retry)
        if close:
            self.close()
        return value

    def items(self, as_dict=False):
        """Get an iterable list of pair-tuples. Alternately get as dict."""

        items = []
        for key in list(self.cache):
            items.append((key, self.get(key, close=False)))
        self.close()

        if as_dict:
            return dict(items)
        return items

    def get_startswith(self, match_elts,  match_part=False, as_dict=False, keys_only=False, suffix_keys=False):
        """
        Get key-value pairs where DataStore cache key starts with a partial key `match_elts`.

        Example `match_elts`:
            match_elts = ['cfg', 'Job.Foo']   # As a list
            match_elts = ('cfg', 'Job.Foo')   # As a tuple
            match_elts = 'cfg:Job.Foo'        # As a string, assuming self.delim == ':'

        :param match_elts: list, str, int, or float of element(s) to match.
        :param bool match_part: Do not add delimiter to last element in match_elts. Default: False
        :param bool as_dict: Convert to dict before returning? Default: False
        :param bool keys_only: Return only a list of keys? Default: False
        :param bool suffix_keys: Return the NOT matching endswith as keys (`as_dict` ignored)? Default: False
        :returns: list of key-value tuples, or list of keys if `keys_only`, or dict if `as_dict`
        """

        if isinstance(match_elts, (list, tuple)):
            match_key = self.makey(*match_elts)
        elif isinstance(match_elts, (str, int, float)):
            match_key = self.makey(match_elts)
        else:
            return []

        # Add the delim to match_key to match WHOLE key prefix like 'cfg:Job.Poop:'
        # Do not add delim to match PARTIAL key prefix like 'cfg:Job.'
        if not match_part:
            match_key += self.delim

        matches = []
        for key in list(self.cache):
            if key.startswith(match_key):
                # Only suffix? Match 'cfg:Job.' gets 'cfg:Job.Poop:key1' returns 'Poop:key1'
                matched_key = key if not suffix_keys else key[len(match_key):]
                if keys_only:
                    matches.append(matched_key)
                else:
                    matches.append((matched_key, self.cache[key]))

        self.close()

        if as_dict and not keys_only:
            return dict(matches)
        return matches

    ############################################################################
    ## Setters
    ############################################################################

    def set(self, key, value, expire=None, read=False, tag=None, retry=True, close=True):
        """Close-able wrapper for Diskcache Cache.set() method."""

        if isinstance(key, (tuple, list)):
            key = self.makey(*key)

        expire = expire or self.expire
        self.cache.set(key, value, expire=expire, read=read, tag=tag, retry=retry)
        if close:
            self.close()

    def add(self, key, value, expire=None, read=False, tag=None, retry=True, close=True):
        """Close-able wrapper for Diskcache Cache.add() method."""

        if isinstance(key, (tuple, list)):
            key = self.makey(*key)

        expire = expire or self.expire
        self.cache.add(key, value, expire=expire, read=read, tag=tag, retry=retry)
        if close:
            self.close()

    def set_from_dict(self, data_dict, exclude_keys=[], add=False, close=True):
        """Flat set from a key-value dict. Nested objects stored as-is."""

        for key, value in data_dict.items():
            if key in exclude_keys:
                continue

            # Might wanna add instead of set
            if add:
                self.add(key, value, close=False)
            else:
                self.set(key, value, close=False)

        if close:
            self.close()

    ############################################################################
    ## Deleters
    ############################################################################

    def delete(self, key, close=True):
        """Delete the record for this key and optional group_key."""

        if isinstance(key, (tuple, list)):
            key = self.makey(*key)
        if key not in self.cache:
            raise KeyError(key)

        del self.cache[key]
        if close:
            self.close()

    def clear(self, close=True):
        """Close-able wrapper for DiskCache clear() method."""

        self.cache.clear()
        if close:
            self.close()

    def clear_all(self):
        """Legacy method from ExpiringDictionary. Just calls self.clear()."""

        self.clear()

    def clear_keys(self, startswith, close=True):
        """Delete any keys matching the given startswith string."""

        for key in self.get_startswith(startswith, keys_only=True):
            del self.cache[key]
        if close:
            self.close()
