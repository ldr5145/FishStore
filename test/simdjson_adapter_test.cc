#include "core/fishstore.h"
#include "gtest/gtest.h"
#include "adapters/simdjson_adapter.h"
#include <filesystem>
#include <vector>
#include <iostream>
#include <string.h>
using namespace fishstore::core;
using adapter_t = fishstore::adapter::SIMDJsonAdapter;
using parser_t = fishstore::adapter::SIMDJsonParser;
using record_t = fishstore::adapter::SIMDJsonRecord;
using field_t = fishstore::adapter::SIMDJsonField;
const char* pattern =
"{\"id\": \"%zu\", \"name\": \"name%zu\", \"gender\": \"%s\", \"school\": {\"id\": \"%zu\", \"name\": \"school%zu\"}}";
const char* singleRecord(R"({"id":"9048418250","type":"IssueCommentEvent","actor":{"id":30538765,"login":"signcla-test-unsigned","display_login":"signcla-test-unsigned","gravatar_id":"","url":"https://api.github.com/users/signcla-test-unsigned","avatar_url":"https://avatars.githubusercontent.com/u/30538765?"},"repo":{"id":98467418,"name":"google-test/signcla-probe-repo","url":"https://api.github.com/repos/google-test/signcla-probe-repo"},"payload":{"action":"created","issue":{"url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/160778","repository_url":"https://api.github.com/repos/google-test/signcla-probe-repo","labels_url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/160778/labels{/name}","comments_url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/160778/comments","events_url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/160778/events","html_url":"https://github.com/google-test/signcla-probe-repo/pull/160778","id":409036198,"node_id":"MDExOlB1bGxSZXF1ZXN0MjUyMTExNTM0","number":160778,"title":"New PR to test SignCLA at 2019-02-11 15:59:25.834691992 -0800 PST m=+2.490587307","user":{"login":"signcla-test-unsigned","id":30538765,"node_id":"MDQ6VXNlcjMwNTM4NzY1","avatar_url":"https://avatars3.githubusercontent.com/u/30538765?v=4","gravatar_id":"","url":"https://api.github.com/users/signcla-test-unsigned","html_url":"https://github.com/signcla-test-unsigned","followers_url":"https://api.github.com/users/signcla-test-unsigned/followers","following_url":"https://api.github.com/users/signcla-test-unsigned/following{/other_user}","gists_url":"https://api.github.com/users/signcla-test-unsigned/gists{/gist_id}","starred_url":"https://api.github.com/users/signcla-test-unsigned/starred{/owner}{/repo}","subscriptions_url":"https://api.github.com/users/signcla-test-unsigned/subscriptions","organizations_url":"https://api.github.com/users/signcla-test-unsigned/orgs","repos_url":"https://api.github.com/users/signcla-test-unsigned/repos","events_url":"https://api.github.com/users/signcla-test-unsigned/events{/privacy}","received_events_url":"https://api.github.com/users/signcla-test-unsigned/received_events","type":"User","site_admin":false},"labels":[{"id":671833561,"node_id":"MDU6TGFiZWw2NzE4MzM1NjE=","url":"https://api.github.com/repos/google-test/signcla-probe-repo/labels/cla:%20no","name":"cla: no","color":"d93f0b","default":false}],"state":"open","locked":false,"assignee":null,"assignees":[],"milestone":null,"comments":2,"created_at":"2019-02-11T23:59:26Z","updated_at":"2019-02-11T23:59:59Z","closed_at":null,"author_association":"COLLABORATOR","pull_request":{"url":"https://api.github.com/repos/google-test/signcla-probe-repo/pulls/160778","html_url":"https://github.com/google-test/signcla-probe-repo/pull/160778","diff_url":"https://github.com/google-test/signcla-probe-repo/pull/160778.diff","patch_url":"https://github.com/google-test/signcla-probe-repo/pull/160778.patch"},"body":"Body of PR"},"comment":{"url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/comments/462545073","html_url":"https://github.com/google-test/signcla-probe-repo/pull/160778#issuecomment-462545073","issue_url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/160778","id":462545073,"node_id":"MDEyOklzc3VlQ29tbWVudDQ2MjU0NTA3Mw==","user":{"login":"signcla-test-unsigned","id":30538765,"node_id":"MDQ6VXNlcjMwNTM4NzY1","avatar_url":"https://avatars3.githubusercontent.com/u/30538765?v=4","gravatar_id":"","url":"https://api.github.com/users/signcla-test-unsigned","html_url":"https://github.com/signcla-test-unsigned","followers_url":"https://api.github.com/users/signcla-test-unsigned/followers","following_url":"https://api.github.com/users/signcla-test-unsigned/following{/other_user}","gists_url":"https://api.github.com/users/signcla-test-unsigned/gists{/gist_id}","starred_url":"https://api.github.com/users/signcla-test-unsigned/starred{/owner}{/repo}","subscriptions_url":"https://api.github.com/users/signcla-test-unsigned/subscriptions","organizations_url":"https://api.github.com/users/signcla-test-unsigned/orgs","repos_url":"https://api.github.com/users/signcla-test-unsigned/repos","events_url":"https://api.github.com/users/signcla-test-unsigned/events{/privacy}","received_events_url":"https://api.github.com/users/signcla-test-unsigned/received_events","type":"User","site_admin":false},"created_at":"2019-02-11T23:59:59Z","updated_at":"2019-02-11T23:59:59Z","author_association":"COLLABORATOR","body":"unsigned-fail-fork: test succeeded"}},"public":true,"created_at":"2019-02-12T00:00:00Z","org":{"id":9579481,"login":"google-test","gravatar_id":"","url":"https://api.github.com/orgs/google-test","avatar_url":"https://avatars.githubusercontent.com/u/9579481?"}}
)");
const char* multiRecord(R"({"id":"9048418250","type":"IssueCommentEvent","actor":{"id":30538765,"login":"signcla-test-unsigned","display_login":"signcla-test-unsigned","gravatar_id":"","url":"https://api.github.com/users/signcla-test-unsigned","avatar_url":"https://avatars.githubusercontent.com/u/30538765?"},"repo":{"id":98467418,"name":"google-test/signcla-probe-repo","url":"https://api.github.com/repos/google-test/signcla-probe-repo"},"payload":{"action":"created","issue":{"url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/160778","repository_url":"https://api.github.com/repos/google-test/signcla-probe-repo","labels_url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/160778/labels{/name}","comments_url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/160778/comments","events_url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/160778/events","html_url":"https://github.com/google-test/signcla-probe-repo/pull/160778","id":409036198,"node_id":"MDExOlB1bGxSZXF1ZXN0MjUyMTExNTM0","number":160778,"title":"New PR to test SignCLA at 2019-02-11 15:59:25.834691992 -0800 PST m=+2.490587307","user":{"login":"signcla-test-unsigned","id":30538765,"node_id":"MDQ6VXNlcjMwNTM4NzY1","avatar_url":"https://avatars3.githubusercontent.com/u/30538765?v=4","gravatar_id":"","url":"https://api.github.com/users/signcla-test-unsigned","html_url":"https://github.com/signcla-test-unsigned","followers_url":"https://api.github.com/users/signcla-test-unsigned/followers","following_url":"https://api.github.com/users/signcla-test-unsigned/following{/other_user}","gists_url":"https://api.github.com/users/signcla-test-unsigned/gists{/gist_id}","starred_url":"https://api.github.com/users/signcla-test-unsigned/starred{/owner}{/repo}","subscriptions_url":"https://api.github.com/users/signcla-test-unsigned/subscriptions","organizations_url":"https://api.github.com/users/signcla-test-unsigned/orgs","repos_url":"https://api.github.com/users/signcla-test-unsigned/repos","events_url":"https://api.github.com/users/signcla-test-unsigned/events{/privacy}","received_events_url":"https://api.github.com/users/signcla-test-unsigned/received_events","type":"User","site_admin":false},"labels":[{"id":671833561,"node_id":"MDU6TGFiZWw2NzE4MzM1NjE=","url":"https://api.github.com/repos/google-test/signcla-probe-repo/labels/cla:%20no","name":"cla: no","color":"d93f0b","default":false}],"state":"open","locked":false,"assignee":null,"assignees":[],"milestone":null,"comments":2,"created_at":"2019-02-11T23:59:26Z","updated_at":"2019-02-11T23:59:59Z","closed_at":null,"author_association":"COLLABORATOR","pull_request":{"url":"https://api.github.com/repos/google-test/signcla-probe-repo/pulls/160778","html_url":"https://github.com/google-test/signcla-probe-repo/pull/160778","diff_url":"https://github.com/google-test/signcla-probe-repo/pull/160778.diff","patch_url":"https://github.com/google-test/signcla-probe-repo/pull/160778.patch"},"body":"Body of PR"},"comment":{"url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/comments/462545073","html_url":"https://github.com/google-test/signcla-probe-repo/pull/160778#issuecomment-462545073","issue_url":"https://api.github.com/repos/google-test/signcla-probe-repo/issues/160778","id":462545073,"node_id":"MDEyOklzc3VlQ29tbWVudDQ2MjU0NTA3Mw==","user":{"login":"signcla-test-unsigned","id":30538765,"node_id":"MDQ6VXNlcjMwNTM4NzY1","avatar_url":"https://avatars3.githubusercontent.com/u/30538765?v=4","gravatar_id":"","url":"https://api.github.com/users/signcla-test-unsigned","html_url":"https://github.com/signcla-test-unsigned","followers_url":"https://api.github.com/users/signcla-test-unsigned/followers","following_url":"https://api.github.com/users/signcla-test-unsigned/following{/other_user}","gists_url":"https://api.github.com/users/signcla-test-unsigned/gists{/gist_id}","starred_url":"https://api.github.com/users/signcla-test-unsigned/starred{/owner}{/repo}","subscriptions_url":"https://api.github.com/users/signcla-test-unsigned/subscriptions","organizations_url":"https://api.github.com/users/signcla-test-unsigned/orgs","repos_url":"https://api.github.com/users/signcla-test-unsigned/repos","events_url":"https://api.github.com/users/signcla-test-unsigned/events{/privacy}","received_events_url":"https://api.github.com/users/signcla-test-unsigned/received_events","type":"User","site_admin":false},"created_at":"2019-02-11T23:59:59Z","updated_at":"2019-02-11T23:59:59Z","author_association":"COLLABORATOR","body":"unsigned-fail-fork: test succeeded"}},"public":true,"created_at":"2019-02-12T00:00:00Z","org":{"id":9579481,"login":"google-test","gravatar_id":"","url":"https://api.github.com/orgs/google-test","avatar_url":"https://avatars.githubusercontent.com/u/9579481?"}}
{"id":"9048418256","type":"WatchEvent","actor":{"id":1524158,"login":"liuyami","display_login":"liuyami","gravatar_id":"","url":"https://api.github.com/users/liuyami","avatar_url":"https://avatars.githubusercontent.com/u/1524158?"},"repo":{"id":146982852,"name":"lcxfs1991/wx-js-utils","url":"https://api.github.com/repos/lcxfs1991/wx-js-utils"},"payload":{"action":"started"},"public":true,"created_at":"2019-02-12T00:00:00Z"}
{"id":"9048418257","type":"PushEvent","actor":{"id":11226087,"login":"GamzeTurkmen","display_login":"GamzeTurkmen","gravatar_id":"","url":"https://api.github.com/users/GamzeTurkmen","avatar_url":"https://avatars.githubusercontent.com/u/11226087?"},"repo":{"id":170221964,"name":"GamzeTurkmen/Distance-Sensor-Project","url":"https://api.github.com/repos/GamzeTurkmen/Distance-Sensor-Project"},"payload":{"push_id":3296313320,"size":1,"distinct_size":1,"ref":"refs/heads/master","head":"86d10f10f18f2b55f512b3f3c5728321008ed588","before":"37fc75a2b4c34f3bb8851dd52fd47aac8d088336","commits":[{"sha":"86d10f10f18f2b55f512b3f3c5728321008ed588","author":{"name":"GamzeTurkmen","email":"b39c1ea5b79f924e66f91b83ac543da8764450e6@hotmail.com"},"message":"Distance Sensor","distinct":true,"url":"https://api.github.com/repos/GamzeTurkmen/Distance-Sensor-Project/commits/86d10f10f18f2b55f512b3f3c5728321008ed588"}]},"public":true,"created_at":"2019-02-12T00:00:00Z"}
{"id":"9048418261","type":"PushEvent","actor":{"id":500941,"login":"ipolevoy","display_login":"ipolevoy","gravatar_id":"","url":"https://api.github.com/users/ipolevoy","avatar_url":"https://avatars.githubusercontent.com/u/500941?"},"repo":{"id":10385373,"name":"javalite/activeweb","url":"https://api.github.com/repos/javalite/activeweb"},"payload":{"push_id":3296313323,"size":1,"distinct_size":1,"ref":"refs/heads/master","head":"8780d10c11b31c9232c18d6fdf8d3394cf15ce09","before":"9a5178050e0dfb86c3d0b31ba5a0764f4351dea1","commits":[{"sha":"8780d10c11b31c9232c18d6fdf8d3394cf15ce09","author":{"name":"ipolevoy","email":"78b6854e22ab09d4ae3dac29b92052963103b33e@expresspigeon.com"},"message":"#415 AbstractLesscController needs to allow additional arguments for Lessc compiler","distinct":true,"url":"https://api.github.com/repos/javalite/activeweb/commits/8780d10c11b31c9232c18d6fdf8d3394cf15ce09"}]},"public":true,"created_at":"2019-02-12T00:00:00Z","org":{"id":1663358,"login":"javalite","gravatar_id":"","url":"https://api.github.com/orgs/javalite","avatar_url":"https://avatars.githubusercontent.com/u/1663358?"}}
)");

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
TEST(Adapter, Load_One) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	//const std::vector<std::string> fields;
	const char* payload = singleRecord;
	adapter_t adapter;
	parser_t* parser = adapter.NewParser({});
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	ASSERT_TRUE(adapter.HasNext(parser));
	ASSERT_EQ(payload_len, parser->len_);
}

TEST(Adapter, Load_Multiple) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const char* payload = multiRecord;
	adapter_t adapter;
	parser_t* parser = adapter.NewParser({});
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	ASSERT_EQ(payload_len, parser->len_);
	ASSERT_TRUE(adapter.HasNext(parser));
}
TEST(Adapter, Parse_One) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const std::vector<std::string> expectedIDs = { "9048418250" };
	const std::vector<std::string> expectedTypes = { "IssueCommentEvent" };
	const std::vector<std::string> fields = {"/id","/type"};
	const char* payload = singleRecord;
	adapter_t adapter;
	parser_t* parser = adapter.NewParser(fields);
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	size_t cnt = 0;
	while (adapter.HasNext(parser)) {
		auto nextRecord = adapter.NextRecord(parser);
		auto parsedFields = nextRecord.GetFields();
		ASSERT_EQ(expectedIDs[cnt], parsedFields[0].GetAsString().Value());
		ASSERT_EQ(expectedTypes[cnt], parsedFields[1].GetAsString().Value());
		++cnt;
	}
	ASSERT_EQ(cnt, 1);
}

TEST(Adapter, Parse_Multiple) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const std::vector<std::string> expectedIDs = { "9048418250", "9048418256", "9048418257", "9048418261" };
	const std::vector<std::string> expectedTypes = { "IssueCommentEvent", "WatchEvent", "PushEvent", "PushEvent"};
	const std::vector<std::string> fields = { "/id","/type" };
	const char* payload = multiRecord;
	adapter_t adapter;
	parser_t* parser = adapter.NewParser(fields);
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	size_t cnt = 0;
	std::string result;
	while (adapter.HasNext(parser)) {
		adapter.NextRecord(parser);
		std::vector<field_t> parsedFields = parser->record.GetFields();
		//ASSERT_EQ(expectedIDs[cnt], parsedFields[0].GetAsString().Value());
		//ASSERT_EQ(expectedTypes[cnt], parsedFields[1].GetAsString().Value());
		result = parsedFields[0].GetAsString().Value();
		++cnt;
	}
	ASSERT_EQ(cnt, 4);
}
TEST(Adapter, Missing_Field) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const std::vector<std::string> fields = { "/abcdefg" };
	const char* payload = singleRecord;
	adapter_t adapter;
	parser_t* parser = adapter.NewParser(fields);
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	size_t cnt = 0;
	while (adapter.HasNext(parser)) {
		auto nextRecord = adapter.NextRecord(parser);
		auto parsedFields = nextRecord.GetFields();
		ASSERT_EQ(parsedFields.size(),0);
		++cnt;
	}
	ASSERT_EQ(cnt, 1);
}

TEST(Adapter, Get_Raw_Record) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const char* payload = singleRecord;
	adapter_t adapter;
	parser_t* parser = adapter.NewParser({});
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	auto record = adapter.NextRecord(parser);
	const char* rawText = record.GetRawText().Data();
	ASSERT_STREQ(rawText, payload);
}

TEST(Adapter, Get_Int_Field) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const char* payload = singleRecord;
	std::vector<std::string> fields = { "/actor/id" };
	int64_t expected = 30538765;
	adapter_t adapter;
	parser_t* parser = adapter.NewParser(fields);
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	auto record = adapter.NextRecord(parser);
	auto parsedField = record.GetFields();
	ASSERT_EQ(expected, parsedField[0].GetAsInt().Value());
	ASSERT_EQ(parsedField.size(), 1);
}
TEST(Adapter, Get_Float_Field) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const char* payload = (R"({"floatval":9999999.999})");
	std::vector<std::string> fields = { "/floatval" };
	float expected = (float)9999999.999;
	adapter_t adapter;
	parser_t* parser = adapter.NewParser(fields);
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	auto record = adapter.NextRecord(parser);
	auto parsedField = record.GetFields();
	ASSERT_EQ(expected, parsedField[0].GetAsFloat().Value());
	ASSERT_EQ(parsedField.size(), 1);
}

TEST(Adapter, Get_Double_Field) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const char* payload = (R"({"doubleval":9999999.999})");
	std::vector<std::string> fields = { "/doubleval" };
	double expected = 9999999.999;
	adapter_t adapter;
	parser_t* parser = adapter.NewParser(fields);
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	auto record = adapter.NextRecord(parser);
	auto parsedField = record.GetFields();
	ASSERT_EQ(expected, parsedField[0].GetAsDouble().Value());
	ASSERT_EQ(parsedField.size(), 1);
}

TEST(Adapter, Get_String_Field) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const char* payload = (R"({"stringval":"expected result"})");
	std::vector<std::string> fields = { "/stringval" };
	const char* expected = "expected result";
	adapter_t adapter;
	parser_t* parser = adapter.NewParser(fields);
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	auto record = adapter.NextRecord(parser);
	auto parsedField = record.GetFields();
	ASSERT_STREQ(expected, parsedField[0].GetAsString().Value().data());
	ASSERT_EQ(parsedField.size(), 1);
}

TEST(Adapter, Get_StringRef_Field) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const char* payload = (R"({"stringval":"expected result"})");
	std::vector<std::string> fields = { "/stringval" };
	const char* expected = "expected result";
	adapter_t adapter;
	parser_t* parser = adapter.NewParser(fields);
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	auto record = adapter.NextRecord(parser);
	auto parsedField = record.GetFields();
	auto result = parsedField[0].GetAsStringRef().Value().Data();
	std::cout << result << std::endl;
	ASSERT_STREQ(expected, parsedField[0].GetAsStringRef().Value().Data());
	ASSERT_EQ(parsedField.size(), 1);
}

TEST(Adapter, Get_Bool_Field) {
	std::filesystem::remove_all("test");
	std::filesystem::create_directories("test");
	const char* payload = (R"({"boolval":true})");
	std::vector<std::string> fields = { "/boolval" };
	adapter_t adapter;
	parser_t* parser = adapter.NewParser(fields);
	size_t payload_len = strlen(payload);
	adapter.Load(parser, payload, payload_len);
	auto record = adapter.NextRecord(parser);
	auto parsedField = record.GetFields();
	ASSERT_TRUE(parsedField[0].GetAsBool().Value());
	ASSERT_EQ(parsedField.size(), 1);
}
int main(int argc, char* argv[]) {
	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}