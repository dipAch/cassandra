USE trie_lab;

INSERT INTO events (id, data) VALUES ('user:alice', 'eng');
INSERT INTO events (id, data) VALUES ('user:bob', 'eng');
INSERT INTO events (id, data) VALUES ('user:carol', 'hr');
INSERT INTO events (id, data) VALUES ('user:dave', 'eng');
INSERT INTO events (id, data) VALUES ('user:eve', 'sec');
INSERT INTO events (id, data) VALUES ('user:frank', 'eng');
INSERT INTO events (id, data) VALUES ('user:grace', 'hr');
INSERT INTO events (id, data) VALUES ('user:heidi', 'ops');
INSERT INTO events (id, data) VALUES ('user:ivan', 'eng');
INSERT INTO events (id, data) VALUES ('user:judy', 'ops');

INSERT INTO events (id, data) VALUES ('order:0001', 'pending');
INSERT INTO events (id, data) VALUES ('order:0002', 'shipped');
INSERT INTO events (id, data) VALUES ('order:0003', 'delivered');
INSERT INTO events (id, data) VALUES ('order:0004', 'pending');
INSERT INTO events (id, data) VALUES ('order:0005', 'cancelled');
INSERT INTO events (id, data) VALUES ('order:0006', 'shipped');
INSERT INTO events (id, data) VALUES ('order:0007', 'delivered');
INSERT INTO events (id, data) VALUES ('order:0008', 'pending');
INSERT INTO events (id, data) VALUES ('order:0009', 'shipped');
INSERT INTO events (id, data) VALUES ('order:0010', 'delivered');

INSERT INTO events (id, data) VALUES ('session:aaaa', 'web');
INSERT INTO events (id, data) VALUES ('session:bbbb', 'mobile');
INSERT INTO events (id, data) VALUES ('session:cccc', 'web');
INSERT INTO events (id, data) VALUES ('session:dddd', 'mobile');
INSERT INTO events (id, data) VALUES ('session:eeee', 'web');
INSERT INTO events (id, data) VALUES ('session:ffff', 'web');
INSERT INTO events (id, data) VALUES ('session:gggg', 'mobile');
INSERT INTO events (id, data) VALUES ('session:hhhh', 'web');
INSERT INTO events (id, data) VALUES ('session:iiii', 'mobile');
INSERT INTO events (id, data) VALUES ('session:jjjj', 'web');

INSERT INTO events (id, data) VALUES ('product:laptop-pro', 'electronics');
INSERT INTO events (id, data) VALUES ('product:laptop-air', 'electronics');
INSERT INTO events (id, data) VALUES ('product:phone-x', 'electronics');
INSERT INTO events (id, data) VALUES ('product:phone-plus', 'electronics');
INSERT INTO events (id, data) VALUES ('product:tablet-s', 'electronics');
INSERT INTO events (id, data) VALUES ('product:headphones', 'audio');
INSERT INTO events (id, data) VALUES ('product:speaker', 'audio');
INSERT INTO events (id, data) VALUES ('product:charger', 'accessories');
INSERT INTO events (id, data) VALUES ('product:case', 'accessories');
INSERT INTO events (id, data) VALUES ('product:stand', 'accessories');

INSERT INTO events (id, data) VALUES ('log:2024-01-01', 'info');
INSERT INTO events (id, data) VALUES ('log:2024-01-02', 'warn');
INSERT INTO events (id, data) VALUES ('log:2024-01-03', 'error');
INSERT INTO events (id, data) VALUES ('log:2024-02-01', 'info');
INSERT INTO events (id, data) VALUES ('log:2024-02-15', 'info');
INSERT INTO events (id, data) VALUES ('metric:cpu', 'gauge');
INSERT INTO events (id, data) VALUES ('metric:mem', 'gauge');
INSERT INTO events (id, data) VALUES ('metric:disk', 'gauge');
INSERT INTO events (id, data) VALUES ('metric:net', 'counter');
INSERT INTO events (id, data) VALUES ('metric:latency', 'histogram');
