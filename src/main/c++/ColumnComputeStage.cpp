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
		       const jlong, const jint, jint*, int*);

void mulNoAVX(const int row, const int col,
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


// basically AVX is used to opimize dot product of two vectors shown
// in mulNoAVX
void mulAVXInt(const int row, const int col,
	       jint** colSlabs_nat,
	       jint* rowSlab_nat,
	       const jint rowMask,
	       const jlong rowPosition,
	       const jint colMask,
	       jint* colPositions_nat,
	       int* results) {
  __m256 ymm0, ymm1, ymm2, ymm3, ymm4, ymm5, ymm6;
  for (int c = col - 1; c >= 0; --c) {
    int prod = 0;
    int row_remain = (row - 1) - (row - 1)%16;
    for (int p = (row - 1); p >= row_remain; --p) {
      int v1 = rowSlab_nat[rowMask & (jint)(rowPosition + p)];
      int v2 = colSlabs_nat[c][colMask & (colPositions_nat[c] + p)];
      prod += v1 * v2;
    }

    // upgrade integer to float due to no dot product for int of AVX
    // another implementation is to use "hadd" instructions
    // see example (http://stackoverflow.com/questions/23186348/integer-dot-product-using-sse-avx?rq=1)
    for (int p = row_remain - 1; p >= 0; p -= 16) {
      ymm0 = _mm256_cvtepi32_ps(_mm256_loadu_si256((__m256i *)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + p - 7)])));
      ymm1 = _mm256_cvtepi32_ps(_mm256_loadu_si256((__m256i *)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + p - 15)])));
      ymm2 = _mm256_cvtepi32_ps(_mm256_loadu_si256((__m256i *)&colSlabs_nat[c][colMask & (colPositions_nat[c] + p - 7)]));
      ymm3 = _mm256_cvtepi32_ps(_mm256_loadu_si256((__m256i *)&colSlabs_nat[c][colMask & (colPositions_nat[c] + p - 15)]));

      ymm4 = _mm256_dp_ps(ymm0, ymm2, 255);
      ymm5 = _mm256_dp_ps(ymm1, ymm3, 255);
      ymm6 = _mm256_add_ps(ymm4, ymm5);
      float tmp_prod[8] = {0};
      _mm256_storeu_ps(tmp_prod, ymm6); 
      prod += (int)tmp_prod[0] + (int)tmp_prod[4];
    }	            
    // results[c] = prod;
    memcpy((void*)(results + c), (void*)&prod, sizeof(int));
  }
}

void mulAVXPS(const int row, const int col,
	      jint** colSlabs_nat,
	      jint* rowSlab_nat,
	      const jint rowMask,
	      const jlong rowPosition,
	      const jint colMask,
	      jint* colPositions_nat,
	      int* results) {
  __m256 ymm0, ymm1, ymm2, ymm3, ymm4, ymm5, ymm6;
  for (int c = col - 1; c >= 0; --c) {
    float prod = 0;
    int row_remain = (row - 1) - (row - 1)%16;
    float* v1 = (float*)calloc(1, sizeof(float));
    float* v2 = (float*)calloc(1, sizeof(float));
    for (int p = (row - 1); p >= row_remain; --p) {
      memcpy((void*)v1, (void*)&rowSlab_nat[rowMask & (jint)(rowPosition + p)], sizeof(float));
      memcpy((void*)v2, (void*)&colSlabs_nat[c][colMask & (colPositions_nat[c] + p)], sizeof(float));
      prod += (*v1) * (*v2);
    }

    for (int p = row_remain - 1; p >= 0; p -= 16) {
      ymm0 = _mm256_loadu_ps((float *)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + p - 7)]));
      ymm1 = _mm256_loadu_ps((float *)(&rowSlab_nat[(int)rowMask & ((int)(rowPosition) + p - 15)]));
      ymm2 = _mm256_loadu_ps((float *)&colSlabs_nat[c][colMask & (colPositions_nat[c] + p - 7)]);
      ymm3 = _mm256_loadu_ps((float *)&colSlabs_nat[c][colMask & (colPositions_nat[c] + p - 15)]);
      
      ymm4 = _mm256_dp_ps(ymm0, ymm2, 255);
      ymm5 = _mm256_dp_ps(ymm1, ymm3, 255);
      ymm6 = _mm256_add_ps(ymm4, ymm5);
      float tmp_prod[8] = {0};
      _mm256_storeu_ps(tmp_prod, ymm6); 
      prod += tmp_prod[0] + tmp_prod[4];
    }	            
  
    memcpy((void*)(results + c), (void*)&prod, sizeof(float));
  }
}

void mulAVXPD(const int row, const int col,
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

void mulAVXLong(const int row, const int col,
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

MULAVX mulavxfuncs[5] = {mulAVXInt, mulAVXPS, mulAVXPD, mulAVXLong, mulNoAVX};
	      
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
  mulavxfuncs[typeMask](resRows, colSlabsRow, colSlabs_nat, rowSlab_nat, rowMask,
			rowPosition, colMask, colPositions_nat, tmp_output);
  updateOutputPipesCol(env, outputPipes, tmp_output, colSlabsRow, outMask, cPosOut_nat);

  env->ReleaseIntArrayElements(rowSlab, rowSlab_nat, 0);
  env->ReleaseIntArrayElements(colPositions, colPositions_nat, 0);
  env->ReleaseIntArrayElements(cPosOut, cPosOut_nat, 0);
  free(tmp_output);
  for (int k = 0; k < colSlabsRow; k++) {                               
    free(colSlabs_nat[k]);
  }
  free(colSlabs_nat);
}
