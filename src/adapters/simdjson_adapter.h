// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <unordered_map>
#include <cstdint>
#include <cassert>
#include <vector>

#ifdef _MSC_VER
#define NOMINMAX
#endif

#include <simdjson.h>

#include "adapters/common_utils.h"
//using namespace simdjson;

const size_t DEFAULT_BATCH_SIZE = 1000000;
namespace fishstore {
namespace adapter {

class SIMDJsonField {
public:
    SIMDJsonField(int64_t id, const simdjson::dom::element el) 
    : field_id(id), element(el){}

	inline int64_t FieldId() const {
		return field_id;
	}

	inline NullableBool GetAsBool() const {
        if (element.is_bool()) {
            return NullableBool(bool(element));
        }
        else {
            return NullableBool();
        }
    }

    inline NullableInt GetAsInt() const{
        if (element.is_int64()) {
            return NullableInt(int64_t(element));
        }
        else {
            return NullableInt();
        }
    }

    inline NullableLong GetAsLong() const{
        if (element.is_int64()) {
            return NullableLong(int64_t(element));
        }
        else {
            return NullableLong();
        }
    }

    inline NullableFloat GetAsFloat() const{
        if (element.is_double()) {
            return NullableFloat(static_cast<float>(double(element)));
        }
        else {
            return NullableFloat();
        }
    }

    inline NullableDouble GetAsDouble() const{
		if (element.is_double()) {
			return NullableDouble(double(element));
		}
		else {
			return NullableDouble();
		}
    }

    inline NullableString GetAsString() const{
        if (element.is_string()) {
            auto val = element.get_string().value();
            return NullableString(std::string(val.data(), val.size()));
        }
        else {
            return NullableString();
        }
    }

    inline NullableStringRef GetAsStringRef() const{
        if (element.is_string()) {
            auto val = element.get_string().value();
            //auto a = NullableStringRef(StringRef(static_cast<const char*>(element), len));
            //std::cout << "Has value: " << a.HasValue() << "\tvalue().data(): " << a.Value().Data() << std::endl;
            //std::cout << "element in GetAsStringRef: " << string << std::endl;
            //return NullableStringRef(StringRef(static_cast<const char*>(element), len));
            return NullableStringRef({ val.data(), val.size() });
        }
        else {
            return NullableStringRef();
        }
    }
private:
    int64_t field_id;
    const simdjson::dom::element element;
};

class SIMDJsonRecord {
public:
  friend class SIMDJsonParser;

  SIMDJsonRecord()
      : fields(), original() {}
  SIMDJsonRecord(const char* data, size_t length)
      : original(data, length) {
      fields.clear();
  }
  inline void clear() {
      original.setData(NULL);
      original.setSize(0);
      fields.clear();
  }
  inline const std::vector<SIMDJsonField>& GetFields() const {
      return fields;
  }

 inline StringRef GetRawText() const {
     return original;
  }

private:
    std::vector<SIMDJsonField> fields;
    StringRef original;
};

class SIMDJsonParser {
public:
    SIMDJsonParser(const std::vector<std::string>& field_names, const size_t alloc_bytes = 1LL << 25)
        : fields(field_names), stream(), buffer_(NULL), p(), len_(0), record() {}

    inline void Load(const char* buffer, size_t length) {
        buffer_ = buffer;
        len_ = length;
        p.parse_many(buffer, length, DEFAULT_BATCH_SIZE).get(stream);
        it = stream.begin();
    }

    inline bool HasNext() {
        return (it!=stream.end());
    }

    inline const SIMDJsonRecord& NextRecord() {
        //for (dom::document_stream::iterator i = stream.begin(); i != stream.end(); ++i) {
        //    simdjson_result<dom::element> doc = *i;
        //    //doc.get(doc_val);
        //    for (auto field_id = 0; field_id < fields.size(); ++field_id) {
        //        //std::cout << "field_id: " <<  fields[field_id] << std::endl;
        //        //std::cout << "fields before conditional check: " << fields[field_id] << "\ntype before: " << typeid(fields[field_id]).name() << std::endl;
        //        //const std::string_view s{ fields[field_id] };
        //        //dom::element doc_val;
        //        //std::cout <<"------------------------------\n" << doc << std::endl;
        //        dom::element doc_val;
        //        auto error = doc.at_pointer(fields[field_id]).get(doc_val);
        //        if (!error) {
        //            //std::cout << "doc val: " << doc_val << std::endl;
        //            SIMDJsonField f = SIMDJsonField{ field_id,doc_val };
        //            record.fields.emplace_back(SIMDJsonField{ field_id, doc_val });
        //            auto test = f.GetAsStringRef();
        //            std::cout << "\nfield: " << fields[field_id] << "\ndoc_val: " << doc_val << "\nStringRef value data: " << test.Value().Data() << "\n\n\n";
        //            //if (strcmp("PullRequestEvent", test.Value().Data())) {
        //            //    std::cout << "\nfield: " << fields[field_id] << "\ndoc_val: " << doc_val << "\nStringRef value data: " << test.Value().Data() << "\n\n\n";
        //            //    count++;
        //            //}
        //        }
        //        else {
        //            //doc_val = dom::element{};
        //            //std::cout << "\n Condition failed.\n";
        //        }
        //    }
        //    std::cout << "--------------------------------\n";
        //}
        //has_next = false;
        ////std::cout << count << std::endl;
        //return record;
        //record.clear();
		record.clear();
		record.original.setData(buffer_ + it.current_index());
		auto last_index = it.current_index();
		for (auto& field : fields) {
            simdjson::dom::element val;
            //std::cout << "field: " << field << std::endl;
            auto error = (*it).at_pointer(field).get(val);
            if (!error) {
                //std::cout << "hit. Getting information..." << std::endl;
                record.fields.emplace_back(SIMDJsonField(record.fields.size(), (*it).at_pointer(field).value()));
                //std::cout << "size (passed into SIMDJsonField as field_id): " << record.fields.size() - 1 << std::endl
                //    << "dom::element data passed to SIMDJsonField: " << (*it).at_pointer(field).value() << "\n\n";
            }
		}
        ++it;
		record.original.setSize(it != stream.end() ? it.current_index() - last_index : len_ - last_index);
        //std::cout << "new record.original size (not field): " << record.original.Length() << std::endl;
        //std::cout << it.current_index() << " " << last_index << " " << len_ << "\n\n";
		return record;
    }
private:
    const char* buffer_;
    size_t len_;
    simdjson::dom::parser p;
    simdjson::dom::document_stream stream;
    simdjson::dom::document_stream::iterator it;
    std::vector<std::string> fields;
    SIMDJsonRecord record;
};

class SIMDJsonAdapter {
public:
  typedef SIMDJsonParser parser_t;
  typedef SIMDJsonField field_t;
  typedef SIMDJsonRecord record_t;

  inline static parser_t* NewParser(const std::vector<std::string>& fields) {
	  return new parser_t{ fields };
  }

  inline static void Load(parser_t* const parser, const char* payload, size_t length, size_t offset = 0) {
	  assert(offset <= length);
	  parser->Load(payload + offset, length - offset);
  }

  inline static bool HasNext(parser_t* const parser) {
	  return parser->HasNext();
  }

  inline static const record_t& NextRecord(parser_t* const parser) {
	  return parser->NextRecord();
  }
};

}
}

