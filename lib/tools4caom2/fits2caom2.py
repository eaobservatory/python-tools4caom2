# Copyright (C) 2014-2015 Science and Technology Facilities Council.
# Copyright (C) 2015 East Asian Observatory.
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

from __future__ import print_function

import logging
import os
import shutil
import subprocess
import tempfile

from tools4caom2.error import CAOMError

logger = logging.getLogger(__name__)


def run_fits2caom2(collection,
                   observationID,
                   productID,
                   observation,
                   override_info,
                   file_uris,
                   local_files,
                   workdir,
                   config_file,
                   default_file,
                   caom2_reader,
                   caom2_writer,
                   arg=None,
                   debug=False,
                   big=False,
                   dry_run=False):
    """
    Generic function to format and run the fits2caom2 command.

    Arguments:
    collection    : CAOM collection for this observation
    observationID : CAOM observationID for this observation
    productID     : CAOM productID for this plane
    observation   : CAOM-2 observation object to be updated, or None if
                    this is to be a new observation
    override_info : (general, sections) override info tuple
    file_uris     : list of file URIs
    local_files   : list of local files
    arg           : list of additional fits2caom2 switches
    debug         : (boolean) include --debug switch by default
    big           : True if fits2caom2 job requires extra RAM
    dry_run       : True to skip actual fits2caom2 run

    If fits2caom2 fails, the command will be run again with the additional
    switch --debug, to capture in the log file details necessary to
    debug the problem.

    Returns:
    The new/updated CAOM-2 observation object.
    """

    cwd = os.getcwd()
    tempdir = None
    try:
        # write the override file
        override_file = os.path.join(
            workdir,
            '_'.join([collection, observationID, productID]) + '.override')

        write_fits2caom2_override(override_file, *override_info)

        # create a temporary working directory
        tempdir = tempfile.mkdtemp(dir=workdir)
        (xmlfile_fd, xmlfile) = tempfile.mkstemp(suffix='.xml', dir=tempdir)
        os.close(xmlfile_fd)
        os.chdir(tempdir)

        # build the fits2caom2 command
        cmd = [
            'java',
            ('-Xmx512m' if big else '-Xmx128m'),
            '-jar',
            os.path.join(os.environ['CADC_ROOT'], 'lib', 'fits2caom2.jar'),
            '--collection=' + collection,
            '--observationID=' + observationID,
            '--productID=' + productID,
            '--ignorePartialWCS',
        ]

        if observation is not None:
            with open(xmlfile, 'w') as f:
                caom2_writer.write(observation, f)
            cmd.append('--in=' + xmlfile)

        cmd.extend([
            '--out=' + xmlfile,
            '--config=' + config_file,
            '--default=' + default_file,
            '--override=' + override_file,
            '--uri=' + ','.join(file_uris),
        ])

        if local_files:
            cmd.append('--local=' + ','.join(local_files))

        if debug:
            cmd.append('--debug')

        if arg is not None:
            cmd.extend(arg)

        # run the command
        logger.info('fits2caom2: cmd = "%s"', ' '.join(cmd))

        if not dry_run:
            output = None

            try:
                output = subprocess.check_output(
                    cmd, shell=False, stderr=subprocess.STDOUT)

                if debug:
                    logger.info('output = "%s"', output)

                observation = caom2_reader.read(xmlfile)

            except subprocess.CalledProcessError as e:
                # if the first attempt to run fits2caom2 fails, try again with
                # --debug to capture the full error message

                logger.error('fits2caom2 return code %d', e.returncode)

                if not debug:
                    logger.info('fits2caom2 - rerun in debug mode')
                    cmd.append('--debug')
                    try:
                        subprocess.check_output(
                            cmd, shell=False, stderr=subprocess.STDOUT)
                    except subprocess.CalledProcessError as ee:
                        output = ee.output
                    else:
                        logger.warning(
                            'fits2caom2 did not fail when rerun with --debug')
                        # Should still raise an error because rerunning with
                        # --debug shouldn't have fixed it.  Retrieve the
                        # output from the original run which did fail.
                        output = e.output

                logger.error('output = "%s"', output)

                raise CAOMError('fits2caom2 exited with bad status')

            else:
                logger.info('fits2caom2 run successful')

    finally:
        if not debug:
            os.remove(override_file)

        # clean up FITS files that were not present originally
        os.chdir(cwd)
        if tempdir:
            shutil.rmtree(tempdir)

    return observation


def write_fits2caom2_override(pathname, general, sections):
    """
    Write an override file for fits2caom2.

    The override file is written to the given pathname.  The general
    parameters are written first, followed by those for particular
    sections.  The "sections" argument can be an OrderedDict to ensure
    that the entries are printed in the expected order.  The keys
    become the section identifiers.
    """

    with open(pathname, 'w') as override:
        for key in general:
            print('%-30s = %s' % (key, general[key]), file=override)

        for (name, section) in sections.items():
            print('', file=override)
            print('?' + name, file=override)
            for key in section:
                print('%-30s = %s' % (key, section[key]), file=override)
