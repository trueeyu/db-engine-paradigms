#include <deque>
#include <iostream>

#include "benchmarks/ssb/Queries.hpp"
#include "common/runtime/Hash.hpp"
#include "common/runtime/Types.hpp"
#include "hyper/GroupBy.hpp"
#include "hyper/ParallelHelper.hpp"
#include "tbb/tbb.h"
#include "vectorwise/Operations.hpp"
#include "vectorwise/Operators.hpp"
#include "vectorwise/Primitives.hpp"
#include "vectorwise/QueryBuilder.hpp"
#include "vectorwise/VectorAllocator.hpp"

using namespace runtime;
using namespace std;

namespace ssb {

// select sum(lineorder.lo_revenue)
// from lineorder, part
// where lineorder.lo_partkey = part.p_partkey

NOVECTORIZE std::unique_ptr<runtime::Query> q21_hyper(Database& db, size_t nrThreads) {
   return nullptr;
}

std::unique_ptr<Q21Builder::Q21> Q21Builder::getQuery() {
   using namespace vectorwise;
   auto result = Result();
   previous = result.resultWriter.shared.result->participate();
   auto r = make_unique<Q21>();

   auto part = Scan("part");
   auto lineorder = Scan("lineorder");

   HashJoin(Buffer(lineorder_part, sizeof(pos_t)), conf.joinAll())
       .addBuildKey(Column(part, "p_partkey"),
                    conf.hash_int32_t_col(),
                    primitives::scatter_int32_t_col)
       .addProbeKey(Column(lineorder, "lo_partkey"),
                   conf.hash_int32_t_col(),
                   primitives::keys_equal_int32_t_col);

   FixedAggregation(Expression()
                    .addOp(primitives::aggr_static_plus_int64_t_col,
                               Value(&r->sum_aaa),
                               Column(lineorder, "lo_revenue"))
                    );
   r->rootOp = popOperator();
   return r;
}

std::unique_ptr<runtime::Query> q21_vectorwise(Database& db, size_t nrThreads, size_t vectorSize) {
   using namespace vectorwise;
   WorkerGroup workers(nrThreads);
   vectorwise::SharedStateManager shared;
   std::unique_ptr<runtime::Query> result;

   workers.run([&]() {
     Q21Builder builder(db, shared, vectorSize);
     auto query = builder.getQuery();
     query->rootOp->next();
     std::cout<<query->sum_aaa<<std::endl;
     std::cout<<query->sum_bbb<<std::endl;
   });

   return result;
}

} // namespace ssb
