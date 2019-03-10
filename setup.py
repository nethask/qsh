from setuptools import setup
from Cython.Build import cythonize

setup(
    name='qsh',
    version='0.7.0',
    url='https://github.com/nethask/qsh',
    author='Artyom Knyazev',
    author_email='nethask@gmail.com',
    license='MIT',
    install_requires=['python-dateutil'],
    ext_modules=cythonize('qsh/__init__.py')
)
