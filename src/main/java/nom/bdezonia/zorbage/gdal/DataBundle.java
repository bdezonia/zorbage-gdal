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

import java.util.List;
import java.util.Map;

import nom.bdezonia.zorbage.data.DimensionedDataSource;
import nom.bdezonia.zorbage.type.float32.complex.ComplexFloat32Member;
import nom.bdezonia.zorbage.type.float32.real.Float32Member;
import nom.bdezonia.zorbage.type.float64.complex.ComplexFloat64Member;
import nom.bdezonia.zorbage.type.float64.real.Float64Member;
import nom.bdezonia.zorbage.type.int16.SignedInt16Member;
import nom.bdezonia.zorbage.type.int16.UnsignedInt16Member;
import nom.bdezonia.zorbage.type.int32.SignedInt32Member;
import nom.bdezonia.zorbage.type.int32.UnsignedInt32Member;
import nom.bdezonia.zorbage.type.int8.UnsignedInt8Member;

/**
 * @author Barry DeZonia
 */
public class DataBundle {

	public Map<String,String> chars;
	public List<DimensionedDataSource<UnsignedInt8Member>> uint8s;
	public List<DimensionedDataSource<SignedInt16Member>> int16s;
	public List<DimensionedDataSource<UnsignedInt16Member>> uint16s;
	public List<DimensionedDataSource<SignedInt32Member>> int32s;
	public List<DimensionedDataSource<UnsignedInt32Member>> uint32s;
	public List<DimensionedDataSource<Float32Member>> floats;
	public List<DimensionedDataSource<Float64Member>> doubles;
	public List<DimensionedDataSource<ComplexFloat32Member>> cfloats;  // including c16int
	public List<DimensionedDataSource<ComplexFloat64Member>> cdoubles; // including c32int

	/**
	 * 
	 * @param newInfo
	 * @param results
	 */
	public void merge(DataBundle newInfo) {
	
		if (chars == null) {
			chars = newInfo.chars;
		}
		else {
			if (newInfo.chars != null) {
				for (String key : newInfo.chars.keySet()) {
					chars.put(key, newInfo.chars.get(key));
				}
			}
		}
	
		if (uint8s == null) {
			uint8s = newInfo.uint8s;
		}
		else {
			if (newInfo.uint8s != null) {
				uint8s.addAll(newInfo.uint8s);
			}
		}
	
		if (uint16s == null) {
			uint16s = newInfo.uint16s;
		}
		else {
			if (newInfo.uint16s != null) {
				uint16s.addAll(newInfo.uint16s);
			}
		}
	
		if (int16s == null) {
			int16s = newInfo.int16s;
		}
		else {
			if (newInfo.int16s != null) {
				int16s.addAll(newInfo.int16s);
			}
		}
	
		if (uint32s == null) {
			uint32s = newInfo.uint32s;
		}
		else {
			if (newInfo.uint32s != null) {
				uint32s.addAll(newInfo.uint32s);
			}
		}
	
		if (int32s == null) {
			int32s = newInfo.int32s;
		}
		else {
			if (newInfo.int32s != null) {
				int32s.addAll(newInfo.int32s);
			}
		}
	
		if (floats == null) {
			floats = newInfo.floats;
		}
		else {
			if (newInfo.floats != null) {
				floats.addAll(newInfo.floats);
			}
		}
	
		if (doubles == null) {
			doubles = newInfo.doubles;
		}
		else {
			if (newInfo.doubles != null) {
				doubles.addAll(newInfo.doubles);
			}
		}
	
		if (cfloats == null) {
			cfloats = newInfo.cfloats;
		}
		else {
			if (newInfo.cfloats != null) {
				cfloats.addAll(newInfo.cfloats);
			}
		}
	
		if (cdoubles == null) {
			cdoubles = newInfo.cdoubles;
		}
		else {
			if (newInfo.cdoubles != null) {
				cdoubles.addAll(newInfo.cdoubles);
			}
		}
	}

}
