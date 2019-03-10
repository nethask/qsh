from setuptools import setup, find_packages
from Cython.Build import cythonize

setup(
    name='qsh',
    version='0.7.4',
    packages=find_packages(),
    url='https://github.com/nethask/qsh',
    author='Artyom Knyazev',
    author_email='nethask@gmail.com',
    license='MIT',
    install_requires=['python-dateutil'],
    ext_modules=cythonize("qsh/*.py")
)
