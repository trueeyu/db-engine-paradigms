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

// select sum(lo_revenue), d_year, p_brand1
// from lineorder, "date", part, supplier
// where lo_orderdate = d_datekey
// and lo_partkey = p_partkey
// and lo_suppkey = s_suppkey
// and p_category = 'MFGR#12'
// and s_region = 'AMERICA'
// group by d_year, p_brand1

//                 sort
//
//                groupby
//
//                 join
//                 hash
//
// tablescan             join
// date                  hash
//
//          tablescan        join
//          supplier         hash
//
//                    tablescan tablescan
//                    part      lineorder

NOVECTORIZE std::unique_ptr<runtime::Query> q21_hyper(Database& db,
                                                      size_t nrThreads) {
  auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];
   auto lo_revenues = lo["lo_revenue"].data<types::Numeric<18, 2>>();
   auto lo_shopmode = lo["lo_shopmode"].data<types::Char<10>>();

   using hash = runtime::CRC32Hash;
   const size_t morselSize = 100000;

   const auto zero = types::Numeric<18, 2>::castString("0.00");
   auto groupOp = make_GroupBy<types::Char<10>, types::Numeric<18, 2>, hash>(
       [](auto& acc, auto&& value) { acc += value; }, zero, nrThreads);

   // preaggregation
   tbb::parallel_for(tbb::blocked_range<size_t>(0, lo.nrTuples, morselSize),
                     [&](const tbb::blocked_range<size_t>& r) {
                        auto groupLocals = groupOp.preAggLocals();
                        for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
                           // --- aggregation
                           groupLocals.consume(lo_shopmode[i], lo_revenues[i]);
                        }
                     });

   // --- output
   auto& result = resources.query->result;
   auto revenueAttr = result->addAttribute("revenue", sizeof(types::Numeric<18, 2>));
   auto shopmode_attr = result->addAttribute("lo_shopmode", sizeof(types::Char<10>));

   groupOp.forallGroups([&](auto& groups) {
      // write aggregates to result
      auto block = result->createBlock(groups.size());
      auto revenue = reinterpret_cast<types::Numeric<18, 2>*>(block.data(revenueAttr));
      auto lo_shopmode = reinterpret_cast<types::Char<10>*>(block.data(shopmode_attr));
      for (auto block : groups)
         for (auto& group : block) {
            //std::cout << group.k << " | " << group.v << std::endl;
            *lo_shopmode++ = group.k;
            *revenue++ = group.v;
         }
      block.addedElements(groups.size());
   });

   leaveQuery(nrThreads);
   return std::move(resources.query);
}

std::unique_ptr<Q21Builder::Q21> Q21Builder::getQuery() {
   using namespace vectorwise;
   auto result = Result();
   previous = result.resultWriter.shared.result->participate();
   auto r = make_unique<Q21>();

   auto lineorder = Scan("lineorder");

   HashGroup()
       .addValue(Column(lineorder, "lo_revenue"),
                 primitives::aggr_init_plus_int64_t_col,
                 primitives::aggr_plus_int64_t_col,
                 primitives::aggr_row_plus_int64_t_col,
                 primitives::gather_val_int64_t_col,
                 Buffer(sum_revenue, sizeof(uint64_t)))
       .addValue(Column(lineorder, "lo_extendedprice"),
                 primitives::aggr_init_plus_int64_t_col,
                 primitives::aggr_plus_int64_t_col,
                 primitives::aggr_row_plus_int64_t_col,
                 primitives::gather_val_int64_t_col,
                 Buffer(sum_extend, sizeof(uint64_t)))
       .addValue(Column(lineorder, "lo_ordtotalprice"),
                 primitives::aggr_init_plus_int64_t_col,
                 primitives::aggr_plus_int64_t_col,
                 primitives::aggr_row_plus_int64_t_col,
                 primitives::gather_val_int64_t_col,
                 Buffer(sum_total, sizeof(uint64_t)));

   result.addValue("sum_revenue", Buffer(sum_revenue))
	 .addValue("sum_extend", Buffer(sum_extend))
	 .addValue("sum_total", Buffer(sum_total))
         .finalize();
   r->rootOp = popOperator();
   return r;
}

std::unique_ptr<runtime::Query> q21_vectorwise(Database& db, size_t nrThreads,
                                               size_t vectorSize) {
   using namespace vectorwise;

   using namespace vectorwise;
   WorkerGroup workers(nrThreads);
   vectorwise::SharedStateManager shared;
   std::unique_ptr<runtime::Query> result;
   workers.run([&]() {
      Q21Builder builder(db, shared, vectorSize);
      auto query = builder.getQuery();
      /* auto found = */ query->rootOp->next();
      auto leader = barrier();
      if (leader)
         result = move(
             dynamic_cast<ResultWriter*>(query->rootOp.get())->shared.result);
   });

   return result;
}

} // namespace ssb
