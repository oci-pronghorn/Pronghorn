#if defined(_MSC_VER)
/* Microsoft C/C++-compatible compiler */
#include <intrin.h>
#elif defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
/* GCC-compatible compiler, targeting x86/x86-64 */
#include <x86intrin.h>
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <jni.h>
#include "com_ociweb_pronghorn_stage_math_ColumnComputeStage.h"

// for performance test. usage:
// timespec start, end;
// clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &start);
// clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end);
// printf("computation: %lds:%ldns\n", diff(start, end).tv_sec, diff(start, end).tv_nsec);
#include <time.h>
timespec diff(timespec start, timespec end)
{
  timespec temp;
  if ((end.tv_nsec-start.tv_nsec)<0) {
    temp.tv_sec = end.tv_sec-start.tv_sec-1;
    temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
  } else {
    temp.tv_sec = end.tv_sec-start.tv_sec;
    temp.tv_nsec = end.tv_nsec-start.tv_nsec;
  }
  return temp;
}

// update one column of output pipes
static inline void updateOutputPipesCol(JNIEnv* env, jobjectArray outputPipes, 
					int* newCol, const int colNum,
					const jint outMask,
					jint* cPosOut_nat) {
  // have to copy each row and update element according to column
  // better implement?
  for (int c = colNum - 1; c >= 0; --c) {
    jintArray output_c = (jintArray)env->GetObjectArrayElement(outputPipes, c); 
    int out_len = env->GetArrayLength(output_c);
    jint* buf_c = (jint*)calloc(out_len, sizeof(jint));
    env->GetIntArrayRegion(output_c, 0, out_len, buf_c);   
    buf_c[(cPosOut_nat[c]) & outMask] = newCol[c];
    env->SetIntArrayRegion(output_c, 0, out_len, buf_c); 
    env->SetObjectArrayElement(outputPipes, c, output_c);    
    env->DeleteLocalRef(output_c); 
    free(buf_c);
  }
}

typedef void (*MULAVX)(const int, const int, jint**, jint*, const jint,
		       const jlong, const jint, jint*, const jint,
		       jint*, jint**);

void mulNoOptimization(int row, int col,
		       jint** colSlabs_nat,
		       jint* rowSlab_nat,
		       const jint rowMask,
		       const jlong rowPosition,
		       const jint colMask,
		       jint* colPositions_nat,
		       int* results) {
  for (int c = col - 1; c >= 0; --c) {
    int prod = 0;
    for (int p = row - 1; p >= 0; --p) {
      int v1 = rowSlab_nat[rowMask & (jint)(rowPosition + p)];
      int v2 = colSlabs_nat[c][colMask & (colPositions_nat[c] + p)];
      prod += v1 * v2;
    }	            
    results[c] = prod;
  }
}
	      
JNIEXPORT void JNICALL Java_com_ociweb_pronghorn_stage_math_ColumnComputeStage_goComputeNative( JNIEnv *env, jobject obj,
												jint typeMask, jintArray rowSlab, jlong rowPosition, jint rowMask, jint resRows,
												jobjectArray colSlabs, jintArray colPositions, jint colMask,
												jobjectArray outputPipes, jintArray cPosOut, jint outMask ) {
  // typeMask == 0 for Integers.
  // typeMask == 1 for Floats.
  // typeMask == 2 for Longs.
  // typeMask == 3 for Doubles.
  // typeMask == 4 for Decimals.
  
  // fetch native rowSlab
  jint* rowSlab_nat = env->GetIntArrayElements(rowSlab, 0);    
  jint* colPositions_nat = env->GetIntArrayElements(colPositions, 0);    
  jint* cPosOut_nat = env->GetIntArrayElements(cPosOut, 0);    
  
  // fetch native colSlabs
  jint colSlabsRow = env->GetArrayLength(colSlabs);
  jint** colSlabs_nat = (jint**)calloc(colSlabsRow, sizeof(jint*));
  for (int k = 0; k < colSlabsRow; k++) {                               
    jintArray col_k = (jintArray)env->GetObjectArrayElement(colSlabs, k); 
    int col_len = env->GetArrayLength(col_k);
    colSlabs_nat[k] = (jint*)calloc(col_len, sizeof(jint));
    env->GetIntArrayRegion(col_k, 0, col_len, colSlabs_nat[k]);   
    env->DeleteLocalRef(col_k);                         
  }

  int* tmp_output = (int*)calloc(colSlabsRow, sizeof(int));
  mulNoOptimization(resRows, colSlabsRow, colSlabs_nat, rowSlab_nat, rowMask,
		    rowPosition, colMask, colPositions_nat, tmp_output);
  updateOutputPipesCol(env, outputPipes, tmp_output, colSlabsRow, outMask, cPosOut_nat);
  free(tmp_output);

  env->ReleaseIntArrayElements(rowSlab, rowSlab_nat, 0);
  env->ReleaseIntArrayElements(colPositions, colPositions_nat, 0);
  env->ReleaseIntArrayElements(cPosOut, cPosOut_nat, 0);
  
  for (int k = 0; k < colSlabsRow; k++) {                               
    free(colSlabs_nat[k]);
  }
  free(colSlabs_nat);
}

// integer
void MulAVXInt(const int length,
	       const int size,
	       jint** colSlabs_nat,
	       jint* rowSlab_nat,
	       const jint rowMask,
	       const jlong rowPosition,
	       const jint colMask,
	       jint* colPositions_nat,
	       const jint outMask,
	       jint* cPosOut_nat,
	       jint** buf_output) {
  __m256i ymm0, ymm1, ymm2, ymm3, ymm4, ymm5, ymm6, ymm7,
    ymm8, ymm9, ymm10, ymm11;

  // multiply
  jint* buf_output_i = (jint*)malloc(size * sizeof(jint));
  for (int i = 0; i < length; i += 32) {        
    ymm0 = _mm256_load_si256((__m256i *)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i)]));
    ymm1 = _mm256_load_si256((__m256i *)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i + 8)]));
    ymm2 = _mm256_load_si256((__m256i *)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i + 16)]));
    ymm3 = _mm256_load_si256((__m256i *)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i + 24)]));
    for (int j = 0; j < length; j += 32) {
      ymm4 = _mm256_load_si256((__m256i *)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i)]);
      ymm5 = _mm256_load_si256((__m256i *)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i + 8)]);
      ymm6 = _mm256_load_si256((__m256i *)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i + 16)]);
      ymm7 = _mm256_load_si256((__m256i *)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i + 24)]);
      
      // not consider overflow
      ymm8 = _mm256_mullo_epi32(ymm0, ymm4);
      ymm9 = _mm256_mullo_epi32(ymm1, ymm5);
      ymm10 = _mm256_mullo_epi32(ymm2, ymm6);
      ymm11 = _mm256_mullo_epi32(ymm3, ymm7);

      _mm256_store_si256((__m256i *)&buf_output_i[(int)(cPosOut_nat[j]) & outMask], ymm8); 
      _mm256_store_si256((__m256i *)&buf_output_i[(int)(cPosOut_nat[j + 8]) & outMask], ymm9); 
      _mm256_store_si256((__m256i *)&buf_output_i[(int)(cPosOut_nat[j + 16]) & outMask], ymm10); 
      _mm256_store_si256((__m256i *)&buf_output_i[(int)(cPosOut_nat[j + 24]) & outMask], ymm11); 
    }	        
    buf_output[i] = buf_output_i;
  }

  // elements not be vectorized
  const int length_reduced = length - length%32;
  for (int i = length_reduced; i < length; ++i) {        
    int v1 = rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i)];
    for (int j = length_reduced; j < length; ++j) {
      // int* is = colSlabs_nat[j];
      int v2 = colSlabs_nat[j][colMask & (colPositions_nat[j] + i)];
      buf_output_i[(int)(cPosOut_nat[j]) & outMask] += v1 * v2;
    }	        
    buf_output[i] = buf_output_i;
  }
  free(buf_output_i);

}

// single precision
void MulAVXPS(const int length,
	       const int size,
	       jint** colSlabs_nat,
	       jint* rowSlab_nat,
	       const jint rowMask,
	       const jlong rowPosition,
	       const jint colMask,
	       jint* colPositions_nat,
	       const jint outMask,
	       jint* cPosOut_nat,
	       jint** buf_output) {
  __m256 ymm0, ymm1, ymm2, ymm3, ymm4, ymm5, ymm6, ymm7,
    ymm8, ymm9, ymm10, ymm11;

  // multiply
  jint* buf_output_i = (jint*)malloc(size * sizeof(jint));
  for (int i = 0; i < length; i += 32) {        
    ymm0 = _mm256_load_ps((float*)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i)]));
    ymm1 = _mm256_load_ps((float*)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i + 8)]));
    ymm2 = _mm256_load_ps((float*)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i + 16)]));
    ymm3 = _mm256_load_ps((float*)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i + 24)]));
    for (int j = 0; j < length; j += 32) {
      ymm4 = _mm256_load_ps((float*)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i)]);
      ymm5 = _mm256_load_ps((float*)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i + 8)]);
      ymm6 = _mm256_load_ps((float*)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i + 16)]);
      ymm7 = _mm256_load_ps((float*)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i + 24)]);
      
      // not consider overflow
      ymm8 = _mm256_mul_ps(ymm0, ymm4);
      ymm9 = _mm256_mul_ps(ymm1, ymm5);
      ymm10 = _mm256_mul_ps(ymm2, ymm6);
      ymm11 = _mm256_mul_ps(ymm3, ymm7);

      _mm256_store_ps((float*)&buf_output_i[(int)(cPosOut_nat[j]) & outMask], ymm8); 
      _mm256_store_ps((float*)&buf_output_i[(int)(cPosOut_nat[j + 8]) & outMask], ymm9); 
      _mm256_store_ps((float*)&buf_output_i[(int)(cPosOut_nat[j + 16]) & outMask], ymm10); 
      _mm256_store_ps((float*)&buf_output_i[(int)(cPosOut_nat[j + 24]) & outMask], ymm11); 
    }	        
    buf_output[i] = buf_output_i;
  }

  // elements not be vectorized
  const int length_reduced = length - length%32;
  for (int i = length_reduced; i < length; ++i) {        
    float v1 = (float)rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i)];
    for (int j = length_reduced; j < length; ++j) {
      // int* is = colSlabs_nat[j];
      float v2 = (float)colSlabs_nat[j][colMask & (colPositions_nat[j] + i)];
      float prod_v = v1 * v2;
      memcpy((void*)&buf_output_i[(int)(cPosOut_nat[j]) & outMask], (void*)&prod_v, sizeof(jint));
      //      buf_output_i[(int)(cPosOut_nat[j]) & outMask] += v1 * v2;
    }	        
    buf_output[i] = buf_output_i;
  }
  free(buf_output_i);

}


// double precision
void MulAVXPD(const int length,
	       const int size,
	       jint** colSlabs_nat,
	       jint* rowSlab_nat,
	       const jint rowMask,
	       const jlong rowPosition,
	       const jint colMask,
	       jint* colPositions_nat,
	       const jint outMask,
	       jint* cPosOut_nat,
	       jint** buf_output) {
  __m256d ymm0, ymm1, ymm2, ymm3, ymm4, ymm5, ymm6, ymm7,
    ymm8, ymm9, ymm10, ymm11;

  // multiply
  jint* buf_output_i = (jint*)malloc(size * sizeof(jint));
  for (int i = 0; i < length; i += 32) {        
    ymm0 = _mm256_load_pd((double*)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i)]));
    ymm1 = _mm256_load_pd((double*)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i + 8)]));
    ymm2 = _mm256_load_pd((double*)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i + 16)]));
    ymm3 = _mm256_load_pd((double*)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i + 24)]));
    for (int j = 0; j < length; j += 32) {
      ymm4 = _mm256_load_pd((double*)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i)]);
      ymm5 = _mm256_load_pd((double*)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i + 8)]);
      ymm6 = _mm256_load_pd((double*)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i + 16)]);
      ymm7 = _mm256_load_pd((double*)&colSlabs_nat[j][colMask & (colPositions_nat[j] + i + 24)]);
      
      // not consider overflow
      ymm8 = _mm256_mul_pd(ymm0, ymm4);
      ymm9 = _mm256_mul_pd(ymm1, ymm5);
      ymm10 = _mm256_mul_pd(ymm2, ymm6);
      ymm11 = _mm256_mul_pd(ymm3, ymm7);

      _mm256_store_pd((double*)&buf_output_i[(int)(cPosOut_nat[j]) & outMask], ymm8); 
      _mm256_store_pd((double*)&buf_output_i[(int)(cPosOut_nat[j + 8]) & outMask], ymm9); 
      _mm256_store_pd((double*)&buf_output_i[(int)(cPosOut_nat[j + 16]) & outMask], ymm10); 
      _mm256_store_pd((double*)&buf_output_i[(int)(cPosOut_nat[j + 24]) & outMask], ymm11); 
    }	        
    buf_output[i] = buf_output_i;
  }

  // elements not be vectorized
  const int length_reduced = length - length%32;
  for (int i = length_reduced; i < length; i = i + 2) {        
    double v1;
    memcpy(&v1, &rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i)], sizeof(jdouble)); 
    for (int j = length_reduced; j < length; ++j) {
      // int* is = colSlabs_nat[j];
      double v2;
      memcpy(&v2, &colSlabs_nat[j][colMask & (colPositions_nat[j] + i)], sizeof(jdouble));
      double prod_v = v1 * v2;
      memcpy((void*)&buf_output_i[(int)(cPosOut_nat[j]) & outMask], (void*)&prod_v, sizeof(jdouble));
    }	        
    buf_output[i] = buf_output_i;
  }
  free(buf_output_i);

}

// long. No AVX support on multiplication of long type
void MulAVXLong(const int length,
		const int size,
		jint** colSlabs_nat,
		jint* rowSlab_nat,
		const jint rowMask,
		const jlong rowPosition,
		const jint colMask,
		jint* colPositions_nat,
		const jint outMask,
		jint* cPosOut_nat,
		jint** buf_output) {
  jint* buf_output_i = (jint*)malloc(size * sizeof(jint));
  for (int i = 0; i < length; i = i + 2) {        
    long v1;
    memcpy(&v1, &rowSlab_nat[(int)rowMask & ((int)(rowPosition) + i)], sizeof(jlong)); 
    for (int j = 0; j < length; j = j + 2) {
      long v2;
      memcpy(&v2, &colSlabs_nat[j][colMask & (colPositions_nat[j] + i)], sizeof(jlong));
      long prod_v = v1 * v2;
      memcpy((void*)&buf_output_i[(int)(cPosOut_nat[j]) & outMask], (void*)&prod_v, sizeof(jlong));
    }	        
    buf_output[i] = buf_output_i;
  }
  free(buf_output_i);
}

// TODO: decimal 
MULAVX mulavxfuncs[5] = {MulAVXInt, MulAVXPS, MulAVXPD, MulAVXLong, MulAVXInt};

JNIEXPORT void JNICALL Java_com_ociweb_pronghorn_stage_math_ColumnComputeStage_goComputeNativeAVX( JNIEnv *env, jobject obj,
												   jint typeMask, jintArray rowSlab, jlong rowPosition, jint rowMask, jint length,
												   jobjectArray colSlabs, jint colSlabsCol, jintArray colPositions, jint colMask,
												   jobjectArray outputPipes, jint outputPipesCol, jintArray cPosOut, jint outMask ) {
  // fetch native rowSlab
  jint* rowSlab_nat = env->GetIntArrayElements(rowSlab, 0);    
  jint* colPositions_nat = env->GetIntArrayElements(colPositions, 0);    
  jint* cPosOut_nat = env->GetIntArrayElements(cPosOut, 0);    
  
  // fetch native colSlabs
  jint size = env->GetArrayLength(colSlabs);
  jint** colSlabs_nat = (jint**)malloc(size * sizeof(jint*));
  for (int k = 0; k < size; k++) {                               
    colSlabs_nat[k] = (jint*)malloc(size * sizeof(jint));
    jintArray col_k = (jintArray)env->GetObjectArrayElement(colSlabs, k); 
    env->GetIntArrayRegion(col_k, 0, size, colSlabs_nat[k]);   
    env->DeleteLocalRef(col_k);                         
  }

  // tmp buffer for mutlication results
  jint** buf_output = (jint**)malloc(size * sizeof(jint));
  jint output_length = env->GetArrayLength(outputPipes);
  
  mulavxfuncs[typeMask](length, output_length, 
			colSlabs_nat,
			rowSlab_nat,
			rowMask,
			rowPosition,
			colMask,
			colPositions_nat,
			outMask,
			cPosOut_nat,
			buf_output);
  env->ReleaseIntArrayElements(rowSlab, rowSlab_nat, 0);
  free(colSlabs_nat);
  free(colPositions_nat);
  free(cPosOut_nat);

  // copy buffer back output pipe
  for (int i = 0; i < length; ++i) {    
    jintArray output_row = env->NewIntArray(size);
    env->SetIntArrayRegion(output_row, 0, size, buf_output[i]); 
    env->SetObjectArrayElement(outputPipes, i, output_row);    
    env->DeleteLocalRef(output_row);                          
  }

  free(buf_output);
}




