from distutils.core import setup

VERSION = '1.0.0'

setup(
    name='denim',
    version=VERSION,
    author='Jeff Ober',
    author_email='jeffober@gmail.com',
    description='Denim is a robust distributed computing platform for Python',
    long_descrption='''
Denim is a complete solution for a managed distributed computing platform. It
focuses on reliability, robustness, speed, and predictable degradation of
performance under stress.
''',
    packages=[
        'denim',
        'denim.actors',
        'denim.protocol',
    ],
    install_requires=([
        'tornado',
    ])
)
