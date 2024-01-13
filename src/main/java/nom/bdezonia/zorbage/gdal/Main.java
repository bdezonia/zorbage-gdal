/*
 * zorbage-gdal: code for using the gdal data file library to open files into zorbage data structures for further processing
 *
 * Copyright (C) 2020-2022 Barry DeZonia
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package nom.bdezonia.zorbage.gdal;

import nom.bdezonia.zorbage.misc.DataBundle;

/**
 * 
 * @author Barry DeZonia
 *
 */
public class Main {

	/*public*/ static void main(String[] args) {
		
		Gdal.init();

		//String filename = "/home/bdz/images/modis/modis.hdf";
		//String filename = "/home/bdz/images/qesdi/cru_v3_dtr_clim10.nc";
		//String filename = "/home/bdz/images/qesdi/wwf_olson2006_ecosystems.nc";
		
		String filename = "/home/bdz/images/imagej-samples/AuPbSn40.jpg";
		//String filename = "/home/bdz/images/imagej-samples/bat-cochlea-renderings.tif";
		//String filename = "/home/bdz/images/imagej-samples/bat-cochlea-volume.tif";
		//String filename = "/home/bdz/images/imagej-samples/blobs.gif";
		//String filename = "/home/bdz/images/imagej-samples/boats.gif";
		//String filename = "/home/bdz/images/imagej-samples/bridge.gif";
		// dicom not supported in gdal
		//String filename = "/home/bdz/images/imagej-samples/Cardio.dcm";
		//String filename = "/home/bdz/images/imagej-samples/Cell_Colony.jpg";
		//String filename = "/home/bdz/images/imagej-samples/clown.jpg";
		//String filename = "/home/bdz/images/imagej-samples/confocal-series.tif";
		// dicom not supported in gdal
		//String filename = "/home/bdz/images/imagej-samples/CT.dcm";
		//String filename = "/home/bdz/images/imagej-samples/Dot_Blot.jpg";
		//String filename = "/home/bdz/images/imagej-samples/embryos.jpg";
		//String filename = "/home/bdz/images/imagej-samples/FluorescentCells.tif";
		//String filename = "/home/bdz/images/imagej-samples/flybrain.tif";
		//String filename = "/home/bdz/images/imagej-samples/gel.gif";
		//String filename = "/home/bdz/images/imagej-samples/hela-cells.tif";
		//String filename = "/home/bdz/images/imagej-samples/leaf.jpg";
		//String filename = "/home/bdz/images/imagej-samples/LineGraph.jpg";
		//String filename = "/home/bdz/images/imagej-samples/m51.tif";
		//String filename = "/home/bdz/images/imagej-samples/mri-stack.tif";
		//String filename = "/home/bdz/images/imagej-samples/NileBend.jpg";
		//String filename = "/home/bdz/images/imagej-samples/organ-of-corti.tif";
		//String filename = "/home/bdz/images/imagej-samples/particles.gif";
		//String filename = "/home/bdz/images/imagej-samples/Rat_Hippocampal_Neuron.tif";
		//String filename = "/home/bdz/images/imagej-samples/mitosis.tif";
		//String filename = "/home/bdz/images/imagej-samples/t1-head.tif";
		//String filename = "/home/bdz/images/imagej-samples/t1-rendering.tif";
		//String filename = "/home/bdz/images/imagej-samples/TEM_filter_sample.jpg";
		//String filename = "/home/bdz/images/imagej-samples/Tree_Rings.jpg";

		DataBundle bundle = Gdal.readAllDatasets(filename);

		System.out.println(bundle.bundle().size() + " datasets were loaded");
	}
}
