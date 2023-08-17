

class MockRados {
public:
  void set_mgr_command_ret(string json_service_dump) {
    service_dump.clear();
    service_dump.append(json_service_dump);
  }

  int mgr_command(string cmd, const bufferlist& inbl,
                  bufferlist* outbl, string* outs) {
    outbl->clear();
    outbl->append(service_dump);
    return 0;
  }

  uint64_t get_instance_id() {
    return (uint64_t)9999;
  }

private:
  bufferlist service_dump;
};

