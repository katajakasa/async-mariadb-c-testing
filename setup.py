from setuptools import setup

setup(
    setup_requires=["cffi>=1.0.0"],
    cffi_modules=["build_ext.py:ffibuilder"],
    install_requires=["cffi>=1.0.0"],
)
