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

   auto customer = Scan("supplier");
   auto lineorder = Scan("lineorder");

   HashJoin(Buffer(lineorder_part, sizeof(pos_t)), conf.joinAll())
       .addBuildKey(Column(customer, "s_suppkey"),
                    conf.hash_int32_t_col(),
                    primitives::scatter_int32_t_col)
      .addProbeKey(Column(lineorder, "lo_suppkey"),
                   conf.hash_int32_t_col(),
                   primitives::keys_equal_int32_t_col);
//      .addBuildValue(Column(lineorder, "lo_revenue"),
//                     primitives::scatter_int64_t_col,
//                     Buffer(p_brand1, sizeof(types::Numeric<18, 2>)),
//                     primitives::gather_col_int64_t_col);

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
