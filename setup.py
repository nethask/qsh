from setuptools import setup, find_packages, Extension

setup(
    name='qsh',
    version='0.8.2',
    packages=find_packages(),
    url='https://github.com/nethask/qsh',
    author='Artyom Knyazev',
    author_email='nethask@gmail.com',
    license='MIT',
    install_requires=['python-dateutil'],
    ext_modules=[Extension('qsh.__init__', ['qsh/__init__.c'])]
)
