# Copyright (C) 2014-2015 Science and Technology Facilities Council.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__ = "Russell O. Redman"


from contextlib import contextmanager
from io import BytesIO
import logging

from caom2.xml.caom2_observation_reader import ObservationReader
from caom2.xml.caom2_observation_writer import ObservationWriter
from caom2.caom2_observation_uri import ObservationURI
from caom2repoClient import CAOM2RepoClient, CAOM2RepoError, CAOM2RepoNotFound

from tools4caom2 import __version__
from tools4caom2.error import CAOMError

__doc__ = """
The caom2repo_wrapper class immplements methods to collect metadata from the
CAOM-2 repository to get, put and update a CAOM-2 observation, implemented
using caom2repoClient.

Version: """ + __version__.version

logger = logging.getLogger(__name__)


class ObservationWrapper(object):
    """
    Class to contain one CAOM-2 observation object.
    """

    def __init__(self, observation):
        self.observation = observation


class Repository(object):
    """
    Wrapper manager class for the caom2repoClient utility.

    Public Interface:
    There are only three public methods
    1) The constructor
    2) process, a context manager for use in a with statement
    3) remove, to remove an observation from the repository

    The get and put methods are nominally private, and the implementation may
    change to suit the details of the caom2repoClient class.

    Notes:
    The caom2repoClient has four methods to get, put, update and remove
    an observation.  The get, put and update actions require that state be
    maintained:

    * If the observation does exist, the final call to push the observation
      back into the repository must be an update.

    * If an observation does not exist in the repository, the final call to
      push the observation into the repository must be a put.
    """

    def __init__(self):
        """
        Create a repository object.
        """

        self.client = CAOM2RepoClient()
        self.reader = ObservationReader(True)
        self.writer = ObservationWriter(True)

    @contextmanager
    def process(self, uri, allow_remove=False, dry_run=False):
        """
        Context manager to fetch and store a CAOM-2 observation.

        Arguments:
        uri: a CAOM-2 URI identifing an observation that may or may not exist
        allow_remove: if the updated observation is empty (contains no planes)
        then the observation is removed.  Otherwise this is an error.
        dry_run: disable putting the replacement observation if true

        Yields:
        An ObservationWrapper either containing the observation, if it already
        exists in CAOM-2, or None otherwise.  The new/updated/replacement
        observation should be placed back into this container in the
        body of the with block.  If None is placed into the container then
        no update will be performed.

        Exceptions:
        CAOMError
            on failure

        Usage:
        Pseudocode illustrating the intended usage::

            repository = Repository()
            for observationID in mycollection:
                uri = <make uri from collection and observationID>
                with repository.process(uri) as wrapper:
                    if wrapper.observation is None:
                        wrapper.observation = SimpleObservation(...)
                    <perform some operation>(wrapper.observation)
        """

        wrapper = ObservationWrapper(self.get(uri))
        exists = wrapper.observation is not None

        yield wrapper

        if wrapper.observation is not None:
            if len(wrapper.observation.planes) == 0:
                # All planes have been removed from the observation: can
                # we remove it?
                if allow_remove:
                    # Only need to remove it if it already existed.
                    if exists:
                        logger.info('No planes left: removing record %s', uri)

                        if not dry_run:
                            self.remove(uri)

                else:
                    # If removal wasn't allowed, raise an error.
                    raise CAOMError(
                        'processed CAOM-2 record contains no planes')

            else:
                # There are planes: put/update the observation.
                if not dry_run:
                    self.put(uri, wrapper.observation, exists)

    def get(self, uri):
        """
        Get an observation from the CAOM-2 repository

        Arguments:
        uri: a CAOM-2 URI identifing an observation that may or may not exist.

        Returns:
        The CAOM-2 observation object, or None if it does not exist yet.

        Exceptions:
        CAOMError
            on failure to fetch the observation (but not if the only error
            is that it doesn't exist).
        """

        if isinstance(uri, ObservationURI):
            myuri = uri.uri
        elif isinstance(uri, str):
            myuri = uri
        else:
            myuri = str(uri)

        try:
            logger.debug('Getting CAOM-2 record: %s', myuri)
            xml = self.client.get_xml(myuri)

            logger.debug('Parsing CAOM-2 record')
            with BytesIO(xml) as f:
                observation = self.reader.read(f)

            return observation

        except CAOM2RepoNotFound:
            logger.debug('CAOM-2 record not found')
            return None

        except CAOM2RepoError:
            logger.exception('error fetching observation from CAOM-2')
            raise CAOMError('failed to fetch observation from CAOM-2')

        except Exception as e:
            logger.exception(
                'unexpected exception fetching observation from CAOM-2')
            raise CAOMError('failed to fetch observation from CAOM-2')

    def put(self, uri, observation, exists):
        """
        Put or update an observation into the CAOM-2 repository.

        Arguments:
        uri: the CAOM-2 URI of the observation
        observation: the CAOM-2 observation object
        exists: if True, use update, else use put

        Exceptions:
        CAOMError on failure to write the observation to the repository
        """

        if isinstance(uri, ObservationURI):
            myuri = uri.uri
        elif isinstance(uri, str):
            myuri = uri
        else:
            myuri = str(uri)

        logger.debug('Serializing CAOM-2 record')
        with BytesIO() as f:
            self.writer.write(observation, f)
            xml = f.getvalue()

        try:
            if exists:
                logger.debug('Updating CAOM-2 record: %s', myuri)
                self.client.update_xml(myuri, xml)
            else:
                logger.debug('Putting new CAOM-2 record: %s', myuri)
                self.client.put_xml(myuri, xml)

        except CAOM2RepoError:
            logger.exception('error putting/updating observation in CAOM-2')
            raise CAOMError('failed to put/update observation in CAOM-2')

    def remove(self, uri):
        """
        Remove an observation from the CAOM-2 repository.

        Arguments:
        uri: the CAOM-2 URI of the observation

        Exceptions:
        CAOMError on failure
        """

        if isinstance(uri, ObservationURI):
            myuri = uri.uri
        elif isinstance(uri, str):
            myuri = uri
        else:
            myuri = str(uri)

        try:
            logger.debug('Removing CAOM-2 record: %s', myuri)
            self.client.remove(myuri)

        except CAOM2RepoError:
            logger.exception('error removing observation from CAOM-2')
            raise CAOMError('failed to remove observation from CAOM-2')
