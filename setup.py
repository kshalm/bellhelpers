from setuptools import setup

setup(name='bellhelper',
      version='0.1',
      description='Helper functions for the Bell Test',
      url='',
      author='Krister Shalm, Gautam Kavuri',
      author_email='lks@nist.gov',
      license='MIT',
      packages=['bellhelper'],
      install_requires=['pyyaml',
                        'bellMotors @ git+https://github.com/kshalm/motorLib.git#egg=bellMotors',
                        'numpy',
                        'scipy',
                        'redis'],
      include_package_data=True,
      zip_safe=False)
