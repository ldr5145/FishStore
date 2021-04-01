#include "core/fishstore.h"
#include "gtest/gtest.h"
#include "adapters/simdjson_adapter.h"
#include <filesystem>
#include <vector>
#include <iostream>
using namespace fishstore::core;
using adapter_t = fishstore::adapter::SIMDJsonAdapter;
using parser_t = fishstore::adapter::SIMDJsonParser;
using record_t = fishstore::adapter::SIMDJsonRecord;
const char* pattern =
"{\"id\": \"%zu\", \"name\": \"name%zu\", \"gender\": \"%s\", \"school\": {\"id\": \"%zu\", \"name\": \"school%zu\"}}";

TEST(Adapter, Create_Parser) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const std::vector<std::string> fields = {"/field1","/field2/subfield2"};
	adapter_t adapter;
	parser_t* parser = adapter.NewParser(fields);
	int cnt = 0;
	for (cnt; cnt < parser->fields.size(); ++cnt);
	ASSERT_EQ(cnt, fields.size());
}
TEST(Adapter, Load_Small) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const std::vector<std::string> fields;
	char buf[1024];
	size_t len = 0;
	for (int i = 0; i < 10; ++i) {
		len += sprintf(buf+len, pattern, i, i, i % 2 ? "M" : "F", i % 2, i % 2);
	}
	adapter_t adapter;
	parser_t* parser = adapter.NewParser({});
	adapter.Load(parser, buf, len);
	ASSERT_TRUE(adapter.HasNext(parser));
}
TEST(Adapter, Load_Large) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	adapter_t adapter;
	parser_t* parser = adapter.NewParser({});
	const std::vector<std::string> fields;
	char buf[1024];
	size_t len = 0;
	for (int i = 0; i < 100000000; ++i) {
		len = sprintf(buf, pattern, i, i, i % 2 ? "M" : "F", i % 10, i % 10);
		adapter.Load(parser, buf, len);
	}
	adapter.Load(parser, buf, len);
	ASSERT_TRUE(adapter.HasNext(parser));
}
TEST(Adapter, Single_Document) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	adapter_t adapter;
	parser_t* parser = adapter.NewParser({ "/id" });
	record_t record;
	char buf[1024];
	size_t len = 0;
	len += sprintf(buf, pattern, 1, 1, "M", 1, 1);
	adapter.Load(parser, buf, len);
	size_t cnt = 0;
	while (adapter.HasNext(parser)) {
		record = adapter.NextRecord(parser);
		ASSERT_EQ(record.GetFields()[cnt].GetAsInt().Value(), cnt);
		cnt++;
	}
}
TEST(Adapter, Multiple_Documents) {
	ASSERT_EQ(1, 1);
}
TEST(Adapter, Invalid_Field) {
	ASSERT_EQ(1, 1);
}
TEST(Adapter, Large_File) {
	ASSERT_EQ(1, 1);
}
int main(int argc, char* argv[]) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}