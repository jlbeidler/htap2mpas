from distutils.core import setup
setup(
    name="htap2mpas",
    version="0.1.0",
    packages=['htap2mpas',],
    scripts = ['bin/htap2mpas',],
    package_data = {'htap2mpas': ['ancillary/*',]}, 
    python_requires='>3.9',
    setup_requires=['numpy==1.26.4','netCDF4==1.7.1.post2','pandas==2.2.2','scipy==1.13.1'],
    author_email='james.beidler@gmail.com'
)
