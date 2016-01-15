from distutils.core import setup

setup(name = 'tdwe',
      version = '0.1',
      description = 'Python interface to the Thomson Reuters Dataworks Enterprise (Datastream) API',
      url = 'http://github.com/QunatForward/tdwe',
      # download_url = '', 
      author = 'Ferenc Szalai',
      author_email = 'szferi@quantforward.com',
      license = 'Apache 2',
      packages = ['tdwe'],
      install_requires = ['suds-jurko', 'pandas']
     )
