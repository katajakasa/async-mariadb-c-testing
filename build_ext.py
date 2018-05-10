import os

from cffi import FFI

ffi = FFI()

CURRENT_DIR = os.path.dirname(__file__)

with open(os.path.join(CURRENT_DIR, "mariadb.c")) as f:
    ffi.set_source(
        "aiomariadb._mariadb_async",
        f.read(),
        libraries=["libmariadb", "ws2_32"],
        include_dirs=["C:\\Program Files\\MariaDB 10.2\\include\\mysql"],
        library_dirs=["C:\\Program Files\\MariaDB 10.2\\lib"]
    )

with open(os.path.join(CURRENT_DIR, "mariadb.h")) as f:
    ffi.cdef(f.read())

if __name__ == "__main__":
    ffi.compile()
