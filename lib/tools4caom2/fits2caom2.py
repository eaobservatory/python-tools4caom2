# TODO: insert copyright here
#
# TODO: insert license here
#
# Code extracted from tools4caom2.caom2ingest

import commands
import logging
import os
import shutil
import tempfile

from tools4caom2.error import CAOMError

logger = logging.getLogger(__name__)


def run_fits2caom2(collection,
                   observationID,
                   productID,
                   observation,
                   override_file,
                   uristring,
                   localstring,
                   workdir,
                   config_file,
                   default_file,
                   caom2_reader,
                   caom2_writer,
                   arg='',
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
    override_file : path to override file
    uristring     : (string) comma-separated list of file URIs
    localstring   : comma-separated list of local files
    arg           : (string) additional fits2caom2 switches
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
        # create a temporary working directory
        tempdir = tempfile.mkdtemp(dir=workdir)
        (xmlfile_fd, xmlfile) = tempfile.mkstemp(suffix='.xml', dir=tempdir)
        os.close(xmlfile_fd)
        os.chdir(tempdir)

        # build the fits2caom2 command

        if big:
            cmd = 'java -Xmx512m -jar ${CADC_ROOT}/lib/fits2caom2.jar '
        else:
            cmd = 'java -Xmx128m -jar ${CADC_ROOT}/lib/fits2caom2.jar '

        cmd += ' --collection="' + collection + '"'
        cmd += ' --observationID="' + observationID + '"'
        cmd += ' --productID="' + productID + '"'
        cmd += ' --ignorePartialWCS'

        if observation is not None:
            with open(xmlfile, 'w') as f:
                caom2_writer.write(observation, f)
            cmd += ' --in="' + xmlfile + '"'
        cmd += ' --out="' + xmlfile + '"'

        cmd += ' --config="' + config_file + '"'
        cmd += ' --default="' + default_file + '"'
        cmd += ' --override="' + override_file + '"'
        cmd += ' --uri="' + uristring + '"'
        if localstring:
            cmd += ' --local="' + localstring + '"'

        if debug:
            cmd += ' --debug'

        if arg:
            cmd += ' ' + arg

        # run the command
        logger.info('fits2caom2Interface: cmd = "%s"', cmd)

        if not dry_run:
            status, output = commands.getstatusoutput(cmd)

            # if the first attempt to run fits2caom2 fails, try again with
            # --debug to capture the full error message
            if status:
                logger.info('fits2caom2 return status %d', status)
                if not debug:
                    logger.info('fits2caom2 - rerun in debug mode')
                    cmd += ' --debug'
                    status, output = commands.getstatusoutput(cmd)
                logger.error('output = "%s"', output)
                raise CAOMError('fits2caom2 exited with bad status')

            elif debug:
                logger.info('output = "%s"', output)

            observation = caom2_reader.read(xmlfile)

    finally:
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

    with open(pathname, 'w') as OVERRIDE:
        for key in general:
            print >>OVERRIDE, \
                '%-30s = %s' % (key, general[key])

        for (name, section) in sections.items():
            print >>OVERRIDE
            print >>OVERRIDE, '?' + name
            for key in section:
                print >>OVERRIDE, \
                    '%-30s = %s' % (key, section[key])
