/*
 * zorbage-gdal: code for loading gdal files into zorbage data structures for further processing
 *
 * Copyright (C) 2020 Barry DeZonia
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package nom.bdezonia.zorbage.gdal;

import nom.bdezonia.zorbage.gdal.Gdal.DataBundle;

/**
 * 
 * @author Barry DeZonia
 *
 */
public class Main {

	public static void main(String[] args) {
		Gdal.init();

		//String filename = "/home/bdz/images/modis/modis.hdf";
		//String filename = "/home/bdz/images/qesdi/cru_v3_dtr_clim10.nc";
		String filename = "/home/bdz/images/imagej-samples/clown.jpg";

		DataBundle bundle = Gdal.open(filename);
		
		if (bundle.uint8s != null) System.out.println("one or more ubyte datasets were loaded");
		if (bundle.uint16s != null) System.out.println("one or more ushort datasets were loaded");
		if (bundle.int16s != null) System.out.println("one or more short datasets were loaded");
		if (bundle.uint32s != null) System.out.println("one or more uint datasets were loaded");
		if (bundle.int32s != null) System.out.println("one or more int datasets were loaded");
		if (bundle.floats != null) System.out.println("one or more float datasets were loaded");
		if (bundle.doubles != null) System.out.println("one or more double datasets were loaded");
		if (bundle.cfloats != null) System.out.println("one or more complex float datasets were loaded");
		if (bundle.cdoubles != null) System.out.println("one or more complex double datasets were loaded");
		if (bundle.chars != null) System.out.println("some character data was loaded");
	}
}
