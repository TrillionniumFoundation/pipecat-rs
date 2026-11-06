#define _GNU_SOURCE
/* Standalone read-only ORT reference probe. MIT licensed.
 * Uses fixed precomputed feature bytes; no frontend, device, or Cargo code runs.
 */
#include "onnxruntime_c_api.h"
#include <dlfcn.h>
#include <errno.h>
#include <float.h>
#include <inttypes.h>
#include <math.h>
#include <stdio.h>

#define FEATURE_COUNT 64000
#define FEATURE_BYTES (FEATURE_COUNT * sizeof(float))
#define MAX_DIMS 8

static const OrtApi *api;
static const char *phase = "startup";

static void json_string(FILE *out, const char *text) {
  fputc('"', out);
  for (const unsigned char *p = (const unsigned char *)(text ? text : ""); *p; ++p) {
    if (*p == '"' || *p == '\\') { fputc('\\', out); fputc(*p, out); }
    else if (*p < 0x20) fprintf(out, "\\u%04x", (unsigned)*p);
    else fputc(*p, out);
  }
  fputc('"', out);
}

static void error_message(const char *message) {
  fputs("{\"kind\":\"error\",\"phase\":", stderr);
  json_string(stderr, phase);
  fputs(",\"message\":", stderr);
  json_string(stderr, message);
  fputs("}\n", stderr);
  fflush(stderr);
}

static int ok(OrtStatus *status, const char *operation) {
  if (!status) return 1;
  fputs("{\"kind\":\"ort_error\",\"phase\":", stderr);
  json_string(stderr, phase);
  fputs(",\"operation\":", stderr);
  json_string(stderr, operation);
  fprintf(stderr, ",\"ort_code\":%d,\"message\":", (int)api->GetErrorCode(status));
  json_string(stderr, api->GetErrorMessage(status));
  fputs("}\n", stderr);
  api->ReleaseStatus(status);
  fflush(stderr);
  return 0;
}
#define CHECK(call) do { if (!ok((call), #call)) goto cleanup; } while (0)
#define REQUIRE(condition, message) do { if (!(condition)) { error_message(message); goto cleanup; } } while (0)

struct shape {
  enum ONNXTensorElementDataType type;
  size_t rank;
  int64_t dims[MAX_DIMS];
};

static int read_shape(const OrtTensorTypeAndShapeInfo *info, struct shape *shape) {
  if (!info) { error_message("metadata is not a tensor"); return 0; }
  if (!ok(api->GetTensorElementType(info, &shape->type), "GetTensorElementType") ||
      !ok(api->GetDimensionsCount(info, &shape->rank), "GetDimensionsCount")) return 0;
  if (shape->rank > MAX_DIMS) { error_message("unexpected tensor rank"); return 0; }
  return ok(api->GetDimensions(info, shape->dims, shape->rank), "GetDimensions");
}

static void print_shape(const struct shape *shape) {
  fprintf(stdout, "\"dtype\":%d,\"shape\":[", (int)shape->type);
  for (size_t i = 0; i < shape->rank; ++i)
    fprintf(stdout, "%s%" PRId64, i ? "," : "", shape->dims[i]);
  fputc(']', stdout);
}

int main(int argc, char **argv) {
  _Static_assert(sizeof(float) == 4 && FLT_RADIX == 2 && FLT_MANT_DIG == 24, "IEEE754 float32 required");
  typedef const OrtApiBase *(*get_api_base_fn)(void);
  _Static_assert(sizeof(get_api_base_fn) == sizeof(void *), "POSIX dlsym pointer representation required");
  int exit_code = 1;
  int release_complete = 0;
  size_t completed_cases = 0;
  void *library = NULL;
  OrtEnv *env = NULL;
  OrtSessionOptions *options = NULL;
  OrtSession *session = NULL;
  OrtMemoryInfo *memory = NULL;
  OrtRunOptions *run_options = NULL;
  OrtValue *input = NULL, *output = NULL;
  OrtTypeInfo *input_type = NULL, *output_type = NULL;
  OrtTensorTypeAndShapeInfo *actual_info = NULL;
  OrtAllocator *allocator = NULL;
  char *input_name = NULL, *output_name = NULL;
  float *features = NULL;
  FILE *feature_file = NULL;
  const char *case_names[3] = {"speech", "tone", "silence"};
  const uint16_t endian = 1;

  setvbuf(stdout, NULL, _IOLBF, 0);
  REQUIRE(argc == 6, "usage: probe LIBONNXRUNTIME MODEL SPEECH_F32 TONE_F32 SILENCE_F32");
  REQUIRE(*(const unsigned char *)&endian == 1, "little-endian host required");
  phase = "load_exact_runtime";
  library = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
  REQUIRE(library, dlerror());
  dlerror();
  void *symbol = dlsym(library, "OrtGetApiBase");
  const char *symbol_error = dlerror();
  REQUIRE(symbol && !symbol_error, symbol_error ? symbol_error : "OrtGetApiBase missing");
  get_api_base_fn get_base;
  memcpy(&get_base, &symbol, sizeof(get_base));
  const OrtApiBase *base = get_base();
  REQUIRE(base, "null OrtApiBase");
  api = base->GetApi(20);
  REQUIRE(api, "runtime does not support requested API20");
  const char *version = base->GetVersionString();
  REQUIRE(version && strcmp(version, "1.21.0") == 0, "runtime version is not exactly 1.21.0");
  Dl_info loaded = {0};
  REQUIRE(dladdr(symbol, &loaded) != 0 && loaded.dli_fname, "cannot identify loaded runtime filename");
  fputs("{\"kind\":\"runtime\",\"api_requested\":20,\"header_api\":21,\"version\":", stdout);
  json_string(stdout, version);
  fputs(",\"loaded_filename\":", stdout);
  json_string(stdout, loaded.dli_fname);
  fputs(",\"build_info\":", stdout);
  json_string(stdout, api->GetBuildInfoString());
  fputs("}\n", stdout);

  phase = "create_session";
  CHECK(api->CreateEnv(ORT_LOGGING_LEVEL_WARNING, "smart-turn-reference", &env));
  CHECK(api->DisableTelemetryEvents(env));
  CHECK(api->CreateSessionOptions(&options));
  CHECK(api->SetSessionExecutionMode(options, ORT_SEQUENTIAL));
  CHECK(api->SetInterOpNumThreads(options, 1));
  CHECK(api->SetIntraOpNumThreads(options, 1));
  CHECK(api->SetSessionGraphOptimizationLevel(options, ORT_DISABLE_ALL));
  CHECK(api->CreateSession(env, argv[2], options, &session));
  CHECK(api->CreateCpuMemoryInfo(OrtArenaAllocator, OrtMemTypeCPUInput, &memory));
  CHECK(api->GetAllocatorWithDefaultOptions(&allocator));
  size_t input_count = 0, output_count = 0;
  CHECK(api->SessionGetInputCount(session, &input_count));
  CHECK(api->SessionGetOutputCount(session, &output_count));
  REQUIRE(input_count == 1 && output_count == 1, "expected exactly one model input and one output");
  CHECK(api->SessionGetInputName(session, 0, allocator, &input_name));
  CHECK(api->SessionGetOutputName(session, 0, allocator, &output_name));
  REQUIRE(input_name && strcmp(input_name, "input_features") == 0, "unexpected model input name");
  REQUIRE(output_name && strcmp(output_name, "logits") == 0, "unexpected model output name");
  CHECK(api->SessionGetInputTypeInfo(session, 0, &input_type));
  CHECK(api->SessionGetOutputTypeInfo(session, 0, &output_type));
  const OrtTensorTypeAndShapeInfo *declared_input = NULL, *declared_output = NULL;
  CHECK(api->CastTypeInfoToTensorInfo(input_type, &declared_input));
  CHECK(api->CastTypeInfoToTensorInfo(output_type, &declared_output));
  struct shape input_shape = {0}, output_shape = {0};
  REQUIRE(read_shape(declared_input, &input_shape), "invalid input tensor metadata");
  REQUIRE(read_shape(declared_output, &output_shape), "invalid output tensor metadata");
  REQUIRE(input_shape.type == ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT &&
          input_shape.rank == 3 && (input_shape.dims[0] == 1 || input_shape.dims[0] < 0) &&
          input_shape.dims[1] == 80 && input_shape.dims[2] == 800, "unexpected declared input shape/type");
  REQUIRE(output_shape.type == ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT, "unexpected declared output type");
  fputs("{\"kind\":\"session\",\"execution\":\"ORT_SEQUENTIAL\",\"inter_threads\":1,\"intra_threads\":1,"
        "\"optimization\":\"ORT_DISABLE_ALL\",\"memory\":\"Cpu/Arena/CPUInput\","
        "\"telemetry\":false,\"explicit_providers\":[]}\n", stdout);
  fputs("{\"kind\":\"model_input\",\"name\":", stdout);
  json_string(stdout, input_name); fputc(',', stdout); print_shape(&input_shape); fputs("}\n", stdout);
  fputs("{\"kind\":\"model_output\",\"name\":", stdout);
  json_string(stdout, output_name); fputc(',', stdout); print_shape(&output_shape); fputs("}\n", stdout);
  api->ReleaseTypeInfo(input_type); input_type = NULL;
  api->ReleaseTypeInfo(output_type); output_type = NULL;

  for (size_t case_index = 0; case_index < 3; ++case_index) {
    phase = case_names[case_index];
    features = malloc(FEATURE_BYTES);
    REQUIRE(features, "cannot allocate fixed feature buffer");
    feature_file = fopen(argv[3 + case_index], "rb");
    REQUIRE(feature_file, "cannot open feature input read-only");
    size_t read_count = fread(features, sizeof(float), FEATURE_COUNT, feature_file);
    int extra = fgetc(feature_file);
    REQUIRE(read_count == FEATURE_COUNT && extra == EOF && !ferror(feature_file), "feature file must be exactly 256000 bytes");
    int file_close = fclose(feature_file); feature_file = NULL;
    REQUIRE(file_close == 0, "feature input close failed");
    for (size_t i = 0; i < FEATURE_COUNT; ++i)
      REQUIRE(isfinite(features[i]), "non-finite feature input");
    const int64_t dimensions[3] = {1, 80, 800};
    CHECK(api->CreateTensorWithDataAsOrtValue(memory, features, FEATURE_BYTES, dimensions, 3,
                                             ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT, &input));
    CHECK(api->CreateRunOptions(&run_options));
    const char *input_names[1] = {input_name};
    const char *output_names[1] = {output_name};
    const OrtValue *inputs[1] = {input};
    CHECK(api->Run(session, run_options, input_names, inputs, 1, output_names, 1, &output));
    CHECK(api->GetTensorTypeAndShape(output, &actual_info));
    struct shape actual_shape = {0};
    REQUIRE(read_shape(actual_info, &actual_shape), "invalid output tensor");
    size_t elements = 0;
    CHECK(api->GetTensorShapeElementCount(actual_info, &elements));
    REQUIRE(actual_shape.type == ONNX_TENSOR_ELEMENT_DATA_TYPE_FLOAT && elements == 1,
            "actual output must contain exactly one float32");
    void *data = NULL;
    CHECK(api->GetTensorMutableData(output, &data));
    REQUIRE(data, "null output data");
    float probability;
    memcpy(&probability, data, sizeof(probability));
    REQUIRE(isfinite(probability) && probability >= 0.0f && probability <= 1.0f,
            "probability outside finite [0,1]");
    uint32_t bits;
    memcpy(&bits, &probability, sizeof(bits));
    fputs("{\"kind\":\"prediction\",\"case\":", stdout);
    json_string(stdout, case_names[case_index]);
    fputs(",\"input_file\":", stdout); json_string(stdout, argv[3 + case_index]);
    fprintf(stdout, ",\"input_bytes\":%zu,\"input_shape\":[1,80,800],\"value\":%.17g,"
                    "\"f32_bits\":\"%08" PRIx32 "\",", (size_t)FEATURE_BYTES, (double)probability, bits);
    print_shape(&actual_shape); fputs("}\n", stdout);
    completed_cases++;
    api->ReleaseTensorTypeAndShapeInfo(actual_info); actual_info = NULL;
    api->ReleaseValue(output); output = NULL;
    api->ReleaseValue(input); input = NULL;
    api->ReleaseRunOptions(run_options); run_options = NULL;
    free(features); features = NULL;
  }
  exit_code = 0;

cleanup:
  phase = "cleanup";
  if (feature_file && fclose(feature_file) != 0) { error_message("feature file cleanup failed"); exit_code = 1; }
  if (api) {
    if (actual_info) api->ReleaseTensorTypeAndShapeInfo(actual_info);
    if (output) api->ReleaseValue(output);
    if (input) api->ReleaseValue(input);
    if (run_options) api->ReleaseRunOptions(run_options);
    if (input_type) api->ReleaseTypeInfo(input_type);
    if (output_type) api->ReleaseTypeInfo(output_type);
    if (allocator && input_name) allocator->Free(allocator, input_name);
    if (allocator && output_name) allocator->Free(allocator, output_name);
    if (memory) api->ReleaseMemoryInfo(memory);
    if (session) api->ReleaseSession(session);
    if (options) api->ReleaseSessionOptions(options);
    if (env) api->ReleaseEnv(env);
  }
  free(features);
  if (library && dlclose(library) != 0) { error_message("dlclose failed"); exit_code = 1; }
  release_complete = 1;
  fprintf(stdout, "{\"kind\":\"cleanup\",\"release_calls_returned\":%s,\"completed_cases\":%zu,\"exit_code\":%d}\n",
          release_complete ? "true" : "false", completed_cases, exit_code);
  if (fflush(stdout) != 0 || ferror(stdout)) return 1;
  return exit_code;
}
