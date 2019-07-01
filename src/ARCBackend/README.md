# ARCBackend Python Package

This package contains the code for the ARC Project Backend, specifically it contains modules for:

* ARCBackend.ARChive

   The ARChive module is the backend data-at-rest solution for ARC.  The base implementation uses HDF5 for storage, but ARChive objects can be considered agnostic to this backend, as the objects are interacted with via the ARChive API and translate this to the HDF5 API.  This allows changing the backend format, and version control/migration in the future.

* ARCBackend.ARCdb

   The ARCdb module is the database runtime module.  It handles search and retrieval of ARChive objects.
