from pywps import Process, LiteralInput, LiteralOutput, ComplexInput, UOM, Format
from pywps.app.Common import Metadata
from pywps.validator.mode import MODE
from pywps.inout.formats import FORMATS

import json
import numpy
import xarray

import logging
LOGGER = logging.getLogger("PYWPS")


class Repackage(Process):
    """Repackaging NetCDF files."""

    def __init__(self):
        inputs = [
            ComplexInput('filesIn', 'Input NetCDF Files URL',
                         abstract='Input NetCDF Files URL.',
                         supported_formats=[Format('JSON')],
                         mode=MODE.STRICT)]
        outputs = [
            LiteralOutput('output', 'Output response',
                          abstract='Output response.',
                          data_type='string')]

        super(Repackage, self).__init__(
            self._handler,
            identifier='repackage',
            title='Repackage',
            abstract='Repackage a list of NetCDF files.'
                     'For now returns an ncdump of the merge of the input files.',
            metadata=[
                Metadata('PyWPS', 'https://pywps.org/'),
                Metadata('Birdhouse', 'http://bird-house.github.io/')
            ],
            version='0.1',
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True
        )

    @staticmethod
    def _handler(request, response):
        LOGGER.info("repackage")
        infiles = []
        jfiles = json.loads(request.inputs['filesIn'][0].data)
        infiles.extend(jfiles["files"])

        ds = xarray.open_mfdataset(filename, decode_cf=False, mask_and_scale=False,
                                   decode_times=False, use_cftime=False, parallel=True,
                                   concat_dim="time", data_vars='minimal', coords='minimal', compat='override')
        ds.to_netcdf(self, path="./out.nc", mode='w', format='NETCDF4')

        response.outputs['output'].data = 'Input files: '+str(infiles)
        response.outputs['output'].uom = UOM('unity')
        return response
