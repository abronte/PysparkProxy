from distutils.core import setup

require = []

with open('requirements.txt') as fp:
    requires = fp.read().split('\n')

with open('LICENSE') as fp:
    license = fp.read()

setup(
    name='PysparkProxy',
    version='0.0.1',
    packages=['pyspark_proxy',],
    license=license,
    description='Seamlessly execute pyspark code on remote clusters',
    long_description=open('README.md').read(),
    install_requires=requires,
    python_requires='>=2.7',
    url='https://github.com/abronte/PysparkProxy',
    author='Adam Bronte',
    author_email='adam@bronte.me',
)
