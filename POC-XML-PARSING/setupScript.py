# setup.py
# This setup.py will install the require packages when the pipeline runs using DataFlowRunner
import setuptools
REQUIRED_PACKAGES = ['xmltodict', 'google-cloud-storage']
PACKAGE_NAME = 'XmlScript'
#PACKAGE_VERSION = '0.0.1'
setuptools.setup(
    name=PACKAGE_NAME,
    #version=PACKAGE_VERSION,
    description='Example project',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
